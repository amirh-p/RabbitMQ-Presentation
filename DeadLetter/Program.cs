using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Text;
using System.Text.Json;

// 1. DEDICATED CHANNEL FOR DLQ PROCESSING:
// Isolates failure handling from main application traffic.
await using var client = await RabbitMqClientFactory.CreateChannelAsync("DLQ-Consumer");

// 2. CONSERVATIVE PREFETCH (QoS):
// Setting prefetchCount = 1 ensures messages are processed sequentially one at a time.
// This prevents pulling multiple failed/poison messages into memory simultaneously.
await client.Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(client.Channel);

consumer.ReceivedAsync += async (sender, @event) =>
{
    var message = Encoding.UTF8.GetString(@event.Body.ToArray());
    var headers = @event.BasicProperties.Headers;

    // 3. PARSING AMQP `x-death` HEADER ARRAY:
    // RabbitMQ automatically injects the 'x-death' header when dead-lettering.
    // Because AMQP uses Erlang dynamic types, in .NET it maps to nested IList<object> and IDictionary<string, object>.
    // Strings in header tables are transmitted as UTF-8 byte arrays (byte[]).
    string originalReason = "Unknown";
    string originalExchange = "Unknown";
    string originalRoutingKey = "Unknown";

    if (headers != null && headers.TryGetValue("x-death", out var deathHeader) && deathHeader is IList<object> deathList && deathList.Count > 0)
    {
        // deathList.First() represents the MOST RECENT dead-letter event in chronological history.
        if (deathList.First() is IDictionary<string, object> deathInfo)
        {
            originalReason = Encoding.UTF8.GetString((byte[])deathInfo["reason"]);     // e.g., "rejected", "expired", or "maxlen"
            originalExchange = Encoding.UTF8.GetString((byte[])deathInfo["exchange"]); // Original target exchange

            if (deathInfo.TryGetValue("routing-keys", out var rKeys) && rKeys is IList<object> keysList && keysList.Count > 0)
            {
                originalRoutingKey = Encoding.UTF8.GetString((byte[])keysList[0]);     // Original publishing key
            }
        }
    }

    Console.WriteLine($"\n[DLQ Handler] Received Dead-Lettered Message (Tag: #{@event.DeliveryTag})");
    Console.WriteLine($"  ├─ Reason: {originalReason}");
    Console.WriteLine($"  ├─ Original Exchange: {originalExchange}");
    Console.WriteLine($"  ├─ Original Routing Key: {originalRoutingKey}");
    Console.WriteLine($"  └─ Payload: {message}");

    // 4. PAYLOAD SANITIZATION / MUTATION:
    // Demonstrates "compensating action" or "patching" corrupt data before re-routing.
    message = OrderCreatedEvent.FixedOrder.Description;
    var newBody = JsonSerializer.SerializeToUtf8Bytes(OrderCreatedEvent.FixedOrder);

    // 5. DECISION LOGIC & ACKNOWLEDGMENT PATTERN:
    // - Dead lettered items must either be Acked (dropped from DLQ after fixing/discarding) 
    //   or Nacked (kept in DLQ / moved to long-term storage).
    if (message.Contains("CORRUPT"))
    {
        Console.WriteLine("  [Decision] Payload is permanently unrecoverable. Removing from DLQ.");

        // Acking without re-publishing permanently drops the message from the DLQ (poison message drop).
        await client.Channel.BasicAckAsync(@event.DeliveryTag, multiple: false);
    }
    else
    {
        Console.WriteLine("  [Decision] Attempting to re-publish back to original exchange for reprocessing...");

        // RE-PUBLISHING BACK TO MAIN PIPELINE:
        // Routes the fixed payload back to the original exchange & routing key.
        await client.Channel.BasicPublishAsync(
            exchange: originalExchange,
            routingKey: originalRoutingKey,
            mandatory: false,
            body: newBody
        );

        // ATOMICITY PATTERN (At-Least-Once Re-queue):
        // Only acknowledge and remove from DLQ AFTER the re-publish call succeeds.
        // If the broker crashes right before this Ack, the message will re-process on startup.
        await client.Channel.BasicAckAsync(@event.DeliveryTag, multiple: false);
        Console.WriteLine("  [Decision] Message re-queued successfully.");
    }
};

// 6. CONSUME FROM DLQ:
await client.Channel.BasicConsumeAsync(queue: "orders-dead-letter-queue", autoAck: false, consumer: consumer);

Console.WriteLine("DLQ Consumer running. Press [Enter] to exit...");
Console.ReadKey();