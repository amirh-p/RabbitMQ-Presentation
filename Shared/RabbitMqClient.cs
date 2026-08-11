using RabbitMQ.Client;

namespace Shared;

public sealed record RabbitMqClient(IConnection Connection, IChannel Channel) : IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        await Channel.DisposeAsync();
        await Connection.DisposeAsync();
    }
}
