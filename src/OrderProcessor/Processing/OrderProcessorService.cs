using OrderProcessor.Data;

namespace OrderProcessor.Processing;

public interface IOrderProcessorService
{
    Task ProcessAsync(OrderPlacedEvent evt, CancellationToken cancellationToken);
}

public sealed class OrderProcessorService(
    OrderDbContext dbContext,
    IRepository<Order> orderRepository,
    IRepository<Shipment> shipmentRepository) : IOrderProcessorService
{
    // All or nothing: orders and shipments are committed together, or neither is.
    public async Task ProcessAsync(OrderPlacedEvent evt, CancellationToken cancellationToken)
    {
        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        await orderRepository.UpsertAsync(new Order
        {
            OrderId = evt.OrderId,
            CustomerEmail = evt.CustomerEmail,
            TotalAmount = evt.TotalAmount,
        }, cancellationToken);

        await shipmentRepository.UpsertAsync(new Shipment
        {
            OrderId = evt.OrderId,
            Line1 = evt.ShippingLine1,
            City = evt.ShippingCity,
            Postcode = evt.ShippingPostcode,
        }, cancellationToken);

        // Any exception above skips this, and disposing the transaction rolls both writes back.
        await transaction.CommitAsync(cancellationToken);
    }
}
