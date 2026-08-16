namespace Shared;

public record OrderCreatedEvent(Guid Id, string Description)
{
    public static readonly OrderCreatedEvent ValidOrder1 = new(Guid.CreateVersion7(), "Valid EU Order #1");

    public static readonly OrderCreatedEvent ValidOrder2 = new(Guid.CreateVersion7(), "Valid EU Order #2");

    public static readonly OrderCreatedEvent IgnoredOrder = new(Guid.CreateVersion7(), "Ignored US Order");

    public static readonly OrderCreatedEvent CorruptOrder = new(Guid.Empty, "CORRUPT_PAYLOAD");
}
