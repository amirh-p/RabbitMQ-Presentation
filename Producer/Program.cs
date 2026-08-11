using Producer;
using RabbitMQ.Client;
using Shared;
using System.Net.Mime;
using System.Text;
using System.Text.Json;

await using var client = await RabbitMqClientFactory.CreateChannelAsync("Presentation-Producer");

client.Channel.BasicReturnAsync += async (sender, @event) =>
{
    var message = JsonSerializer.Deserialize<OrderCreatedEvent>(Encoding.UTF8.GetString(@event.Body.ToArray()))!;
    Console.WriteLine($"[Producer] Message returned: '{message.Id} - {message.Description}' | Key: '{@event.RoutingKey}' | Reason : '{@event.ReplyText}'");
};

var eventsToPublish = new RabbitMqMessage<OrderCreatedEvent>[]
{
    new(RoutingKey: "order.created.eu", Body: OrderCreatedEvent.ValidOrder1),
    new(RoutingKey: "order.updated.eu", Body:OrderCreatedEvent.ValidOrder2),
    new(RoutingKey: "order.created.us", Body: OrderCreatedEvent.IgnoredOrder),
    new(RoutingKey: "order.corrupt.eu", Body: OrderCreatedEvent.CorruptOrder),
};

foreach (var item in eventsToPublish)
{
    var props = new BasicProperties
    {
        Persistent = true, //Persist in DISK instead of RAM, so that if the RabbitMQ server crashes, the message is not lost. Equivalent of durable = true for queues.
        ContentType = MediaTypeNames.Application.Json
    };

    await client.Channel.BasicPublishAsync(
        exchange: "orders",
        routingKey: item.RoutingKey,
        mandatory: true, //Tells the broker that if the message cannot be routed to a queue, it should be returned to the sender. If set to false, the message will be dropped if it cannot be routed.
        basicProperties: props,
        body: JsonSerializer.SerializeToUtf8Bytes(item.Body));

    Console.WriteLine($"[Producer] Published: '{item.Body.Id} - {item.Body.Description}' | Key: '{item.RoutingKey}'");
}