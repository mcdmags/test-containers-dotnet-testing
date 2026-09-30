namespace OrderProcessor.Data;

public sealed class Order
{
    public required string OrderId { get; set; }
    public required string CustomerEmail { get; set; }
    public required decimal TotalAmount { get; set; }
}

public sealed class Shipment
{
    public required string OrderId { get; set; }
    public required string Line1 { get; set; }
    public required string City { get; set; }
    public string? Postcode { get; set; }
}
