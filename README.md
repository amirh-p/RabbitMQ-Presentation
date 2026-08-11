# RabbitMQ Presentation

A small .NET example demonstrating a Producer → Topic Exchange → Queue flow in RabbitMQ with routing keys, mandatory returns, dead-lettering (DLX), and batch acknowledgements. Intended as a teaching/demo project to show common RabbitMQ patterns and how a simple producer and consumer interact.

## Highlights
- Topic exchange named `orders`
- Producer publishes messages with routing keys like `order.created.eu`, `order.updated.eu`, `order.created.us`, `order.corrupt.eu`
- A consumer reads from `order-created-queue` which is bound to `orders` using `order.*.eu`
- Dead Letter Exchange `orders.dlx` with a bound `orders-dead-letter-queue`
- Producer uses `mandatory: true` and handles BasicReturn for unroutable messages
- Consumer demonstrates manual ack/nack, batching acks (BATCH_SIZE = 2), and re-routing corrupt messages to DLX

## Stack
- Language: C# (.NET)
- Runtime: .NET SDK (6.0+ recommended)
- RabbitMQ client: RabbitMQ.Client

## Repository layout
Producer/                - Producer project (publishes order events)
  - Program.cs           - Publishes sample messages (routing keys, BasicPublish with mandatory)
  - OrderCreatedEvent.cs - Sample event types used by the producer

Consumer/                - Consumer project (consumes messages from queue)
  - Program.cs           - Async consumer, manual acks, batch ack logic, nack to DLX for corrupt payloads

Shared/                  - Shared code used by both apps
  - RabbitMqClientFactory.cs - Creates connection/channel, declares exchange, queues, bindings, DLX, TTL, etc.
  - RabbitMqClient.cs         - Lightweight wrapper (connection + channel)
  - RabbitMqMessage.cs        - Generic message container (RoutingKey + Body)

Other files:
- RabbitMQPresentation.slnx - Visual Studio solution file
- README.md (this file)

## How it works (runtime flow)
1. Producer creates events (OrderCreatedEvent) and publishes them to exchange `orders` (ExchangeType.Topic).
2. The `order-created-queue` is bound using `order.*.eu`, so only messages whose routing key match that pattern are routed to the queue (EU messages).
3. If a published message cannot be routed and the publish used `mandatory: true`, the server returns the message to the producer; the code registers a BasicReturn handler to log returned messages.
4. The consumer reads messages from `order-created-queue` with `autoAck: false`, uses a small in-memory batch strategy, and issues batched BasicAck (multiple: true) every BATCH_SIZE messages.
5. Messages containing `CORRUPT` are BasicNack'ed with `requeue: false`, so they're delivered to the dead-letter exchange `orders.dlx` and end up in `orders-dead-letter-queue`.

## Quickstart (local)
Prerequisites:
- .NET SDK 6.0 or later
- RabbitMQ broker (local or remote). For local testing you can use the official management image:

Run RabbitMQ locally:
```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```
Open the management UI at: http://localhost:15672 (default user/password: guest/guest)

Build and run:
```bash
# From repository root
dotnet build

# Run the consumer in one terminal (listens on order-created-queue)
dotnet run --project Consumer

# Run the producer in another terminal (publishes sample messages)
dotnet run --project Producer
```

Expected behavior:
- Producer logs published messages and any BasicReturn events (unroutable messages).
- Consumer logs each processed delivery tag and will:
  - Nack corrupt payloads (those containing "CORRUPT") — they go to DLX.
  - Hold messages in memory until a batch of 2 is accumulated and then ack them in bulk.

## Configuration & customization
- Connection settings are currently defined in `Shared/RabbitMqClientFactory.cs` (HostName, Port, UserName, Password, VirtualHost, SSL options). Edit that file to point to a remote broker or replace with environment-based configuration if desired.
- Exchange/queue names and routing keys are declared in the same factory; change them if you want different routing/topology.
- Message persistence: Producer sets BasicProperties.Persistent = true so messages survive broker restarts if queues are durable.
- TTL: The main queue sets `x-message-ttl` (milliseconds) in the factory. Adjust to change message lifetime.

## Key files (quick reference)
- Shared/RabbitMqClientFactory.cs
  - Declares exchanges: `orders` (topic), `orders.dlx` (fanout)
  - Declares queues: `order-created-queue` with args:
    - `x-dead-letter-exchange`: `orders.dlx`
    - `x-message-ttl`: 60000 (ms)
  - Binds `order-created-queue` with routing key `order.*.eu`
- Producer/Program.cs
  - Publishes messages with routing keys:
    - `order.created.eu` (routed)
    - `order.updated.eu` (routed)
    - `order.created.us` (not matched by queue binding → returned to producer if mandatory)
    - `order.corrupt.eu` (contains "CORRUPT")
  - Registers `Channel.BasicReturnAsync` to log returned (unroutable) messages
- Consumer/Program.cs
  - Sets `BasicQos` prefetchCount = 10
  - Uses an async consumer + manual acks and batch ack logic (BATCH_SIZE = 2)
  - Nacks corrupt payloads with `requeue: false` to send them to DLX

## Logs / Example output
Producer:
- [Producer] Published: '...'
- [Producer] Message returned: '... - ...' | Key: 'order.created.us' | Reason: 'NO_ROUTE'

Consumer:
- [Consumer] Processing Tag #123 | RoutingKey: 'order.created.eu'
- [Consumer] Corrupt payload on Tag #124. Nacking to DLX...
- [Consumer] Batch limit (2) reached. Sending BasicAckAsync(Tag: 125, multiple: true)

## Troubleshooting
- If nothing is routed to your queue:
  - Ensure the routing key pattern matches the queue binding (`order.*.eu`)
  - Verify the exchange name `orders` exists and is of type `topic`
- If producer sees returned messages with `NO_ROUTE`, the message didn't match any queue bindings for the exchange
- If consumer immediately loses messages after nack:
  - Verify DLX `orders.dlx` and `orders-dead-letter-queue` are declared and bound

## Extending this demo
- Replace hard-coded connection settings with configuration (IConfiguration / environment variables)
- Add a retry/backoff before sending to DLX instead of immediate nack
- Add monitoring (expose metrics) or persist the dead-letter queue to a separate store for later inspection

## License
MIT — see LICENSE file (or add one)

## Contributing
PRs and improvements welcome — especially for:
- Config-driven connection/topology
- Additional examples showing routing variations and consumer recovery
- Tests for the shared client factory and message handlers
