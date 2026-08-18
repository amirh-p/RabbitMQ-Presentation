using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Text;
using System.Text.Json;

await using var client = await RabbitMqClientFactory.CreateChannelAsync("DLQ-Consumer");

await client.Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(client.Channel);

consumer.ReceivedAsync += async (sender, @event) =>
{
    var message = Encoding.UTF8.GetString(@event.Body.ToArray());
    var headers = @event.BasicProperties.Headers;

    string originalReason = string.Empty;
    string originalExchange = string.Empty;
    string originalRoutingKey = string.Empty;

    if (headers != null && headers.TryGetValue("x-death", out var deathHeader) && deathHeader is IList<object> deathList && deathList.Count > 0)
    {
        if (deathList.First() is IDictionary<string, object> deathInfo)
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

    var newMessage = new RabbitMqMessage<OrderCreatedEvent>("order.created.eu", OrderCreatedEvent.FixedOrder);

    Console.WriteLine("  [Decision] Attempting to re-publish back to original exchange for reprocessing...");

    await client.Channel.BasicPublishAsync(
        exchange: originalExchange,
        routingKey: newMessage.RoutingKey,
        mandatory: false,
        body: JsonSerializer.SerializeToUtf8Bytes(newMessage.Body)
    );

    await client.Channel.BasicAckAsync(@event.DeliveryTag, multiple: false);
    Console.WriteLine("  [Decision] Message re-queued successfully.");
};

await client.Channel.BasicConsumeAsync(queue: "orders-dead-letter-queue", autoAck: false, consumer: consumer);

Console.WriteLine("DLQ Consumer running. Press [Enter] to exit...");
Console.ReadKey();