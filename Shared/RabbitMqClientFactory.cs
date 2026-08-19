using RabbitMQ.Client;

namespace Shared;

public static class RabbitMqClientFactory
{
    public static async Task<RabbitMqClient> CreateChannelAsync(string clientName)
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = "localhost",
            VirtualHost = "/",
            Port = 5672,
            UserName = "guest",
            Password = "guest",

            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
            RequestedHeartbeat = TimeSpan.FromSeconds(60),
            RequestedConnectionTimeout = TimeSpan.FromSeconds(30),

            Ssl = new SslOption
            {
                Enabled = false,
                ServerName = "localhost"
            },

            ClientProvidedName = clientName
        };

        var connection = await connectionFactory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("orders", ExchangeType.Topic, durable: true);

        await channel.ExchangeDeclareAsync("orders.dlx", ExchangeType.Topic, durable: true);
        await channel.QueueDeclareAsync("orders-dead-letter-queue", durable: true, exclusive: false, autoDelete: false);

        await channel.QueueBindAsync("orders-dead-letter-queue", "orders.dlx", routingKey: "#");

        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "orders.dlx" },
            { "x-message-ttl", 300000  }
        };

        await channel.QueueDeclareAsync(
            queue: "order-created-queue",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainQueueArgs);

        await channel.QueueBindAsync(
            queue: "order-created-queue",
            exchange: "orders",
            routingKey: "order.*.eu");

        return new(connection, channel);
    }
}