using Microsoft.EntityFrameworkCore;

namespace OrderProcessor.Data;

public sealed class EfRepository<TEntity>(OrderDbContext dbContext) : IRepository<TEntity> where TEntity : class
{
    // Saves immediately. When called inside OrderProcessorService's transaction the row is written
    // but only becomes visible to others once that transaction commits.
    public async Task UpsertAsync(TEntity entity, CancellationToken cancellationToken)
    {
        var keyValues = dbContext.Model.FindEntityType(typeof(TEntity))!.FindPrimaryKey()!.Properties
            .Select(property => property.PropertyInfo!.GetValue(entity))
            .ToArray();

        var existing = await dbContext.Set<TEntity>().FindAsync(keyValues, cancellationToken);
        if (existing is null)
            dbContext.Add(entity);
        else
            dbContext.Entry(existing).CurrentValues.SetValues(entity);

        await dbContext.SaveChangesAsync(cancellationToken);
    }
}
