using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Moq;
using OrderProcessor.Data;

namespace OrderProcessor.IntegrationTests;

// Black-box tests: publish to Kafka, then read Postgres and the captured logs.
// Nothing here calls the worker or its services directly.
[Collection(IntegrationTestCollection.Name)]
public sealed class OrderPlacedConsumerTests(ContainersFixture containers)
{
    [Fact]
    public async Task Should_Save_Order_And_Shipment_When_Event_Is_Consumed()
    {
        await using var app = await TestFixture.StartAsync(containers);
        var evt = NewEvent();

        await app.PublishAndWaitUntilProcessedAsync(evt);

        await using var db = containers.CreateDbContext();
        var order = await db.Orders.AsNoTracking().SingleAsync(o => o.OrderId == evt.OrderId);
        var shipment = await db.Shipments.AsNoTracking().SingleAsync(s => s.OrderId == evt.OrderId);
        Assert.Equal(("customer@example.com", 42.50m), (order.CustomerEmail, order.TotalAmount));
        Assert.Equal(("1 Main Street", "Springfield", "12345"), (shipment.Line1, shipment.City, shipment.Postcode));
    }

    [Fact]
    public async Task Should_Not_Persist_Either_Table_And_Log_Error_When_Shipment_Upsert_Throws()
    {
        // Only the SECOND write is mocked. The order upsert is the real EF repository and really runs
        // SaveChanges inside the transaction, so an empty orders table proves the rollback happened.
        // Mocking a repository that writes both tables would pass even without a transaction.
        var shipmentRepository = new Mock<IRepository<Shipment>>();
        shipmentRepository
            .Setup(r => r.UpsertAsync(It.IsAny<Shipment>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Simulated shipment upsert failure"));

        await using var app = await TestFixture.StartAsync(containers, services =>
        {
            services.RemoveAll<IRepository<Shipment>>();
            services.AddScoped(_ => shipmentRepository.Object);
        });
        var evt = NewEvent();

        await app.PublishAndWaitUntilProcessedAsync(evt);

        shipmentRepository.Verify(r => r.UpsertAsync(
            It.Is<Shipment>(s => s.OrderId == evt.OrderId), It.IsAny<CancellationToken>()), Times.Once);

        var error = Assert.Single(app.Logs.GetSnapshot(), r =>
            r.Level == LogLevel.Error && r.Category == typeof(OrderPlacedConsumerWorker).FullName);
        Assert.StartsWith($"Rolled back order {evt.OrderId}", error.Message);
        Assert.Equal("Simulated shipment upsert failure", Assert.IsType<InvalidOperationException>(error.Exception).Message);

        await using var db = containers.CreateDbContext();
        Assert.False(await db.Orders.AnyAsync(o => o.OrderId == evt.OrderId));
        Assert.False(await db.Shipments.AnyAsync(s => s.OrderId == evt.OrderId));
    }

    [Fact]
    public async Task Should_Not_Persist_Either_Table_When_Database_Rejects_Shipment()
    {
        // No mocks: postcode is NOT NULL in Postgres, so the real shipment insert fails after the order insert.
        await using var app = await TestFixture.StartAsync(containers);
        var evt = NewEvent(postcode: null);

        await app.PublishAndWaitUntilProcessedAsync(evt);

        var error = Assert.Single(app.Logs.GetSnapshot(), r =>
            r.Level == LogLevel.Error && r.Category == typeof(OrderPlacedConsumerWorker).FullName);
        Assert.StartsWith($"Rolled back order {evt.OrderId}", error.Message);

        await using var db = containers.CreateDbContext();
        Assert.False(await db.Orders.AnyAsync(o => o.OrderId == evt.OrderId));
        Assert.False(await db.Shipments.AnyAsync(s => s.OrderId == evt.OrderId));
    }

    [Fact]
    public async Task Should_Update_Existing_Rows_When_Same_Order_Is_Published_Again()
    {
        await using var app = await TestFixture.StartAsync(containers);
        var first = NewEvent();
        var updated = first with { TotalAmount = 99.99m, ShippingCity = "Shelbyville" };

        await app.PublishAndWaitUntilProcessedAsync(first);
        await app.PublishAndWaitUntilProcessedAsync(updated);

        await using var db = containers.CreateDbContext();
        var order = await db.Orders.AsNoTracking().SingleAsync(o => o.OrderId == first.OrderId);
        var shipment = await db.Shipments.AsNoTracking().SingleAsync(s => s.OrderId == first.OrderId);
        Assert.Equal(99.99m, order.TotalAmount);
        Assert.Equal("Shelbyville", shipment.City);
    }

    // A new Guid per event so each test only sees its own rows in the shared database.
    private static OrderPlacedEvent NewEvent(string? postcode = "12345") =>
        new(Guid.NewGuid().ToString(), "customer@example.com", 42.50m, "1 Main Street", "Springfield", postcode);
}
