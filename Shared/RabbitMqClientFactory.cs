using RabbitMQ.Client;

namespace Shared;

public static class RabbitMqClientFactory
{
    public static async Task<RabbitMqClient> CreateChannelAsync(string clientName)
    {
        var connectionFactory = new ConnectionFactory
        {
            HostName = "localhost",
            VirtualHost = "/", //Example: "/dev", "/prod"
            Port = 5672, //Default port for RabbitMQ
            UserName = "guest",
            Password = "guest",
            AutomaticRecoveryEnabled = true, //Enable automatic recovery of connections if connection drops unexpectedly
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10), //How long to wait between reconnection attempts when automatic recovery kicks in. Prevents hammering the broker with rapid retries.
            RequestedHeartbeat = TimeSpan.FromSeconds(60), //How often the client and broker exchange "heartbeat" signals to detect dead connections. Lower values detect failures faster but add overhead.
            RequestedConnectionTimeout = TimeSpan.FromSeconds(30), //How long to wait for a connection to be established before timing out. Prevents hanging indefinitely if the broker is unreachable.
            Ssl = new SslOption
            {
                Enabled = false, //Set to true if using SSL/TLS
                ServerName = "localhost" //Should match the hostname on the broker's TLS certificate for validation to succeed.
            },
            ClientProvidedName = clientName//This shows up in the RabbitMQ Management UI's connections list, which is very handy for debugging when you have many services connecting — without it, you just see anonymous connections.
        };

        var connection = await connectionFactory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        // 1. Topic Exchange setup
        await channel.ExchangeDeclareAsync("orders", ExchangeType.Topic, durable: true);

        // 2. Dead Letter Exchange (DLX) setup
        await channel.ExchangeDeclareAsync("orders.dlx", ExchangeType.Fanout, durable: true);
        await channel.QueueDeclareAsync("orders-dead-letter-queue", durable: true, exclusive: false, autoDelete: false);
        await channel.QueueBindAsync("orders-dead-letter-queue", "orders.dlx", routingKey: "");

        // 3. Main Queue with DLX enabled
        var mainQueueArgs = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "orders.dlx" }, //Dead Letter Exchange
            { "x-message-ttl", 60000  } //Message TTL in milliseconds (30 seconds)
        };

        await channel.QueueDeclareAsync(
            queue: "order-created-queue",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: mainQueueArgs);

        // Topic binding using wildcard: matches order.created.eu, order.updated.eu, etc.
        await channel.QueueBindAsync(
            queue: "order-created-queue",
            exchange: "orders",
            routingKey: "order.*.eu");

        return  new(connection, channel);
    }
}
