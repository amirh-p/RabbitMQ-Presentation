using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Text;

await using var client = await RabbitMqClientFactory.CreateChannelAsync("Presentation-Consumer");

await client.Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

var consumer = new AsyncEventingBasicConsumer(client.Channel);

int unackedCount = 0;
const int BATCH_SIZE = 2;

consumer.ReceivedAsync += async (sender, @event) =>
{
    var message = Encoding.UTF8.GetString(@event.Body.ToArray());

    Console.WriteLine($"\n[Consumer] Processing Tag #{@event.DeliveryTag} | RoutingKey: '{@event.RoutingKey}' | Message: '{message}'");

    if (message.Contains("CORRUPT"))
    {
        Console.WriteLine($"[Consumer] Corrupt payload on Tag #{@event.DeliveryTag}. Nacking to DLX...");

        await client.Channel.BasicNackAsync(deliveryTag: @event.DeliveryTag, multiple: false, requeue: false);
        return;
    }

    await Task.Delay(100);
    unackedCount++;

    if (unackedCount % BATCH_SIZE == 0)
    {
        Console.WriteLine($"[Consumer] Batch limit ({BATCH_SIZE}) reached. Sending BasicAckAsync(Tag: {@event.DeliveryTag}, multiple: true)");

        await client.Channel.BasicAckAsync(deliveryTag: @event.DeliveryTag, multiple: true);
    }
    else
    {
        Console.WriteLine($"[Consumer] Holding Tag #{@event.DeliveryTag} in memory buffer (Waiting for batch size {BATCH_SIZE})...");
    }
};

await client.Channel.BasicConsumeAsync(queue: "order-created-queue", autoAck: false, consumer: consumer);

Console.WriteLine("Consumer listening. Press [Enter] to exit...");
Console.ReadKey();