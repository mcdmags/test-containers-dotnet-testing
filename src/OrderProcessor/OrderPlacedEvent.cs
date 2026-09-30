namespace OrderProcessor;

public sealed record OrderPlacedEvent(
    string OrderId,
    string CustomerEmail,
    decimal TotalAmount,
    string ShippingLine1,
    string ShippingCity,
    string? ShippingPostcode);
