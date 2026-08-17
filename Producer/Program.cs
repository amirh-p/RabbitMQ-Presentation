using RabbitMQ.Client;
using Shared;
using System.Net.Mime;
using System.Text;
using System.Text.Json;

// 1. CHANNEL CREATION & CONNECTION LIFECYCLE:
// Creates a dedicated channel for publishing over an underlying TCP connection.
// 'await using' ensures proper cleanup and graceful connection termination upon exit.
await using var client = await RabbitMqClientFactory.CreateChannelAsync("Presentation-Producer");

// 2. UNROUTABLE MESSAGE HANDLING (BasicReturn Event):
// Triggers ONLY when publishing with 'mandatory: true' and RabbitMQ cannot route 
// the message to at least ONE queue (e.g., no queue is bound to the exchange with that routing key).
client.Channel.BasicReturnAsync += async (sender, @event) =>
{
    var message = JsonSerializer.Deserialize<OrderCreatedEvent>(Encoding.UTF8.GetString(@event.Body.ToArray()))!;
    Console.WriteLine($"[Producer] Message returned: '{message.Id} - {message.Description}' | Key: '{@event.RoutingKey}' | Reason : '{@event.ReplyText}'");
};

var eventsToPublish = new RabbitMqMessage<OrderCreatedEvent>[]
{
    new(RoutingKey: "order.created.eu", Body: OrderCreatedEvent.ValidOrder1),
    new(RoutingKey: "order.updated.eu", Body: OrderCreatedEvent.ValidOrder2),
    new(RoutingKey: "order.created.us", Body: OrderCreatedEvent.IgnoredOrder),
    new(RoutingKey: "order.corrupt.eu", Body: OrderCreatedEvent.CorruptOrder),
};

foreach (var item in eventsToPublish)
{
    // 3. MESSAGE METADATA & PERSISTENCE:
    var props = new BasicProperties
    {
        // Persistent = true (DeliveryMode = 2): Forces RabbitMQ to write the message to DISK.
        // NOTE: For full durability, the destination queue MUST also be declared as Durable = true.
        Persistent = true,
        ContentType = MediaTypeNames.Application.Json
    };

    // 4. PUBLISHING MECHANICS & GUARANTEES:
    await client.Channel.BasicPublishAsync(
        exchange: "orders",         // Exchanges route messages based on bindings; producers don't publish directly to queues.
        routingKey: item.RoutingKey, // Binding pattern matcher used by Topic/Direct exchanges.

        // mandatory: true => Guarantees "at-least-routed" handling. If no queue matches this key,
        // RabbitMQ returns the message via the BasicReturnAsync event rather than silently dropping it.
        mandatory: true,

        basicProperties: props,
        body: JsonSerializer.SerializeToUtf8Bytes(item.Body));

    Console.WriteLine($"[Producer] Published: '{item.Body.Id} - {item.Body.Description}' | Key: '{item.RoutingKey}'");
}