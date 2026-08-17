using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared;
using System.Text;

// 1. CHANNEL SCOPE & THREAD SAFETY:
// Channels are light connections over a single TCP socket. They are NOT thread-safe for 
// concurrent publishes/acks, but in RabbitMQ.Client v7+, async methods ensure safe dispatching.
await using var client = await RabbitMqClientFactory.CreateChannelAsync("Presentation-Consumer");

// 2. PREFETCH COUNT (QoS / BACKPRESSURE):
// Limits how many unacknowledged messages RabbitMQ pushes to this consumer over the wire.
// - prefetchSize: 0 = No limit on byte size.
// - prefetchCount: 10 = Max 10 unacked messages buffered locally. Prevents memory overload.
// - global: false = Applies PER CONSUMER instance on this channel, NOT shared channel-wide.
await client.Channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false);

var consumer = new AsyncEventingBasicConsumer(client.Channel);

// 3. IN-MEMORY STATE IN ASYNC CALLBACKS:
// 'unackedCount' is modified inside an async delegate. If messages arrive concurrently,
// race conditions can occur. (Here, BasicQos + single consumer processes sequentially by default).
int unackedCount = 0;
const int BATCH_SIZE = 2;

consumer.ReceivedAsync += async (sender, @event) =>
{
    // 4. DELIVERY TAGS & AMQP WIRE ENCODING:
    // - DeliveryTag is a monotonically increasing 64-bit integer SCOPED ONLY TO THIS CHANNEL.
    // - Body comes as ReadOnlyMemory<byte>, requiring explicit UTF-8 decoding.
    var message = Encoding.UTF8.GetString(@event.Body.ToArray());

    Console.WriteLine($"\n[Consumer] Processing Tag #{@event.DeliveryTag} | RoutingKey: '{@event.RoutingKey}'");

    // 5. DEAD-LETTERING (NACK vs REJECT):
    if (message.Contains("CORRUPT"))
    {
        Console.WriteLine($"[Consumer] Corrupt payload on Tag #{@event.DeliveryTag}. Nacking to DLX...");

        // 1. Flush and acknowledge any prior valid messages held in the batch buffer
        if (unackedCount > 0)
        {
            Console.WriteLine($"[Consumer] Flushing {unackedCount} buffered message(s) prior to Tag #{@event.DeliveryTag}...");
            await client.Channel.BasicAckAsync(deliveryTag: @event.DeliveryTag - 1, multiple: true);
            unackedCount = 0;
        }

        // - requeue: false = Triggers Dead Letter Exchange (DLX) routing if configured on the queue.
        //   If no DLX exists, the message is permanently dropped/purged from the broker.
        // - BUG ALERT: Returning here leaves any previously unacked valid messages (held in the batch buffer)
        //   hanging until a new valid message arrives!
        await client.Channel.BasicNackAsync(deliveryTag: @event.DeliveryTag, multiple: false, requeue: false);
        return;
    }

    await Task.Delay(100);
    unackedCount++;

    // 6. BATCH ACKNOWLEDGMENT (multiple: true):
    if (unackedCount % BATCH_SIZE == 0)
    {
        Console.WriteLine($"[Consumer] Batch limit ({BATCH_SIZE}) reached. Sending BasicAckAsync(Tag: {@event.DeliveryTag}, multiple: true)");

        // - multiple: true = Tells RabbitMQ: "Acknowledge THIS delivery tag AND ALL lower tags prior to it."
        //   This reduces network round-trips significantly compared to acking every single message.
        await client.Channel.BasicAckAsync(deliveryTag: @event.DeliveryTag, multiple: true);
    }
    else
    {
        Console.WriteLine($"[Consumer] Holding Tag #{@event.DeliveryTag} in memory buffer (Waiting for batch size {BATCH_SIZE})...");
    }
};

// 7. EXPLICIT ACKNOWLEDGMENT MODE:
// - autoAck: false = Mandatory for reliable messaging. Messages remain in broker RAM/Disk
//   until explicitly Acked/Nacked. If consumer dies before Ack, RabbitMQ re-queues them.
await client.Channel.BasicConsumeAsync(queue: "order-created-queue", autoAck: false, consumer: consumer);

Console.WriteLine("Consumer listening. Press [Enter] to exit...");
Console.ReadKey();