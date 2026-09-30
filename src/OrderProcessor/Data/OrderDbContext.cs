using Microsoft.EntityFrameworkCore;

namespace OrderProcessor.Data;

public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<Shipment> Shipments => Set<Shipment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(order =>
        {
            order.ToTable("orders");
            order.HasKey(o => o.OrderId);
            order.Property(o => o.OrderId).HasColumnName("order_id");
            order.Property(o => o.CustomerEmail).HasColumnName("customer_email");
            order.Property(o => o.TotalAmount).HasColumnName("total_amount").HasPrecision(12, 2);
        });

        modelBuilder.Entity<Shipment>(shipment =>
        {
            shipment.ToTable("shipments");
            shipment.HasKey(s => s.OrderId);
            shipment.Property(s => s.OrderId).HasColumnName("order_id");
            shipment.Property(s => s.Line1).HasColumnName("line1");
            shipment.Property(s => s.City).HasColumnName("city");
            // Nullable in C# but NOT NULL in the database, so a missing postcode fails at INSERT time.
            shipment.Property(s => s.Postcode).HasColumnName("postcode").IsRequired();
            shipment.HasOne<Order>().WithOne().HasForeignKey<Shipment>(s => s.OrderId);
        });
    }
}
