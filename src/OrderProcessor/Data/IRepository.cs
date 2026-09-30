namespace OrderProcessor.Data;

public interface IRepository<TEntity> where TEntity : class
{
    Task UpsertAsync(TEntity entity, CancellationToken cancellationToken);
}
