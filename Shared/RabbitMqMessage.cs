namespace Shared;

public sealed record RabbitMqMessage<TBody>(string RoutingKey, TBody Body);