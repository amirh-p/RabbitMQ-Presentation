using RabbitMQ.Client;

namespace Shared;

public static class RabbitMqClientFactory
{
    public static async Task<RabbitMqClient> CreateChannelAsync(string clientName)
    {
        // 1. CONNECTION CONFIGURATION & RESILIENCE:
        // ConnectionFactory acts as the central client configuration blueprint.
        var connectionFactory = new ConnectionFactory
        {
            HostName = "localhost",
            VirtualHost = "/", // Logical isolated partition inside RabbitMQ (great for multi-tenancy/dev environments).
            Port = 5672,       // Default AMQP plain-text port. (15672 is Web UI, 5671 is SSL/TLS).
            UserName = "guest",
            Password = "guest",

            // AUTOMATIC RECOVERY MECHANICS:
            // Re-establishes broken TCP connections and automatically re-declares exchanges, 
            // queues, bindings, and active consumer subscriptions after network loss.
            AutomaticRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(10), // Prevents connection-storming the broker upon failure.
            RequestedHeartbeat = TimeSpan.FromSeconds(60),       // Detects TCP "half-open" state or dead sockets every 60s.
            RequestedConnectionTimeout = TimeSpan.FromSeconds(30),

            Ssl = new SslOption
            {
                Enabled = false,
                ServerName = "localhost"
            },

            // OBSERVABILITY IN MANAGEMENT UI:
            // Sets a human-readable identifier in the RabbitMQ Web Dashboard under the "Connections" tab.
            ClientProvidedName = clientName
        };

        // 2. CONNECTION vs CHANNEL LIFECYCLE:
        // - Connection = Heavy physical TCP socket to the broker (Reuse across app lifecycle).
        // - Channel = Lightweight virtual multiplexed connection over the TCP socket (1 per thread/task).
        // NOTE: In production, calling CreateConnectionAsync on EVERY factory call creates redundant TCP sockets.
        // Ideally, share 1 IConnection per process and open new IChannels per consumer/producer.
        var connection = await connectionFactory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        // 3. TOPIC EXCHANGE SETUP:
        // Routing strategy based on dot-separated wildcard patterns (* = 1 word, # = 0+ words).
        await channel.ExchangeDeclareAsync("orders", ExchangeType.Topic, durable: true);

        // 4. DEAD LETTER EXCHANGE (DLX) TOPOLOGY:
        // Declares a dedicated Direct exchange and queue for capturing dead-lettered messages (Nack, Expired, MaxLength).
        await channel.ExchangeDeclareAsync("orders.dlx", ExchangeType.Direct, durable: true);
        await channel.QueueDeclareAsync("orders-dead-letter-queue", durable: true, exclusive: false, autoDelete: false);

        // Binds DLQ to DLX with an empty routing key so all dead-lettered messages arrive here regardless of origin key.
        await channel.QueueBindAsync("orders-dead-letter-queue", "orders.dlx", routingKey: "");

        // 5. MAIN QUEUE WITH ADVANCED ARGUMENTS (DLX & TTL):
        var mainQueueArgs = new Dictionary<string, object?>
        {
            // Directs rejected/nacked/expired messages from this queue to the DLX.
            { "x-dead-letter-exchange", "orders.dlx" }, 
            
            // Queue-level Message Time-To-Live (TTL):
            // Unprocessed messages expire after 60,000 ms (60 seconds) and get sent to DLX.
            // (Note: Comment says 30 seconds, but 60000 ms = 60s).
            { "x-message-ttl", 60000  }
        };

        // QUEUE IMMUTABILITY RULE:
        // Queue arguments (like x-dead-letter-exchange, x-message-ttl, durable) are IMMUTABLE once declared.
        // If you change these parameters later, RabbitMQ will throw a PRECONDITION_FAILED exception 
        // until the old queue is deleted or re-declared with identical arguments.
        await channel.QueueDeclareAsync(
            queue: "order-created-queue",
            durable: true,    // Queue survives broker restart (writes queue metadata to disk).
            exclusive: false,  // Accessible by multiple connection instances.
            autoDelete: false, // Queue persists even when all consumers disconnect.
            arguments: mainQueueArgs);

        // 6. TOPIC BINDING PATTERN:
        // Matches routing keys starting with 'order.', ending with '.eu', and having exactly ONE word in between.
        // E.g., Matches: "order.created.eu", "order.updated.eu" | Ignores: "order.created.us", "order.eu"
        await channel.QueueBindAsync(
            queue: "order-created-queue",
            exchange: "orders",
            routingKey: "order.*.eu");

        return new(connection, channel);
    }
}