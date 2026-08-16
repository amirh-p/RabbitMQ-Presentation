using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Text;

await using var client = await RabbitMqClientFactory.CreateChannelAsync("DLQ-Consumer");

await client.Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(client.Channel);

consumer.ReceivedAsync += async (sender, @event) =>
{
    var message = Encoding.UTF8.GetString(@event.Body.ToArray());
    var headers = @event.BasicProperties.Headers;

    // 1. Extract Dead-Letter Metadata automatically attached by RabbitMQ
    string originalReason = "Unknown";
    string originalExchange = "Unknown";
    string originalRoutingKey = "Unknown";

    if (headers != null && headers.TryGetValue("x-death", out var deathHeader) && deathHeader is IList<object> deathList && deathList.Count > 0)
    {
        if (deathList[0] is IDictionary<string, object> deathInfo)
        {
            originalReason = Encoding.UTF8.GetString((byte[])deathInfo["reason"]);
            originalExchange = Encoding.UTF8.GetString((byte[])deathInfo["exchange"]);

            if (deathInfo.TryGetValue("routing-keys", out var rKeys) && rKeys is IList<object> keysList && keysList.Count > 0)
            {
                originalRoutingKey = Encoding.UTF8.GetString((byte[])keysList[0]);
            }
        }
    }

    Console.WriteLine($"\n[DLQ Handler] Received Dead-Lettered Message (Tag: #{@event.DeliveryTag})");
    Console.WriteLine($"  ├─ Reason: {originalReason}");
    Console.WriteLine($"  ├─ Original Exchange: {originalExchange}");
    Console.WriteLine($"  ├─ Original Routing Key: {originalRoutingKey}");
    Console.WriteLine($"  └─ Payload: {message}");

    // 2. Decision Logic: Fix/Sanitize and Reprocess OR Poison-Queue/Discard
    if (message.Contains("CORRUPT"))
    {
        Console.WriteLine("  [Decision] Payload is permanently unrecoverable. Removing from DLQ.");
        // Acknowledge to drop it permanently from DLQ
        await client.Channel.BasicAckAsync(@event.DeliveryTag, multiple: false);
    }
    else
    {
        Console.WriteLine("  [Decision] Attempting to re-publish back to original exchange for reprocessing...");

        // Re-publish message to its original target exchange & routing key
        await client.Channel.BasicPublishAsync(
            exchange: originalExchange,
            routingKey: originalRoutingKey,
            mandatory: false,
            body: @event.Body
        );

        // Ack from DLQ once successfully re-published
        await client.Channel.BasicAckAsync(@event.DeliveryTag, multiple: false);
        Console.WriteLine("  [Decision] Message re-queued successfully.");
    }
};

await client.Channel.BasicConsumeAsync(queue: "orders-dead-letter-queue", autoAck: false, consumer: consumer);

Console.WriteLine("DLQ Consumer running. Press [Enter] to exit...");
Console.ReadKey();

//[
//  {
//    "reason": "rejected",              // "rejected", "expired", or "maxlen"
//    "queue": "order-created-queue",    // Queue the message died in
//    "time": 1723838583,                // Timestamp (Erlang/Unix epoch)
//    "exchange": "orders-exchange",     // Original exchange before dead-lettering
//    "routing-keys": ["order.created"], // Original routing key(s)
//    "count": 1                         // How many times it died for THIS reason in THIS queue
//  }
//][
//  {
//    "reason": "rejected",              // "rejected", "expired", or "maxlen"
//    "queue": "order-created-queue",    // Queue the message died in
//    "time": 1723838583,                // Timestamp (Erlang/Unix epoch)
//    "exchange": "orders-exchange",     // Original exchange before dead-lettering
//    "routing-keys": ["order.created"], // Original routing key(s)
//    "count": 1                         // How many times it died for THIS reason in THIS queue
//  }
//]