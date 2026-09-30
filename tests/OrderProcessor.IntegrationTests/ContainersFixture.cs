using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.EntityFrameworkCore;
using OrderProcessor.Data;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

namespace OrderProcessor.IntegrationTests;

// Shared by every test in the collection: containers are slow to start, so they start once.
// Each test still gets its own app host (TestFixture), topic and consumer group.
public sealed class ContainersFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:16-alpine").Build();
    private readonly KafkaContainer _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.9.0").Build();

    public string PostgresConnectionString => _postgres.GetConnectionString();
    public string KafkaBootstrapAddress => _kafka.GetBootstrapAddress();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _kafka.StartAsync());

        // With EF migrations, use dbContext.Database.MigrateAsync() instead.
        await using var dbContext = CreateDbContext();
        await dbContext.Database.EnsureCreatedAsync();
    }

    public OrderDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<OrderDbContext>().UseNpgsql(PostgresConnectionString).Options);

    public async Task CreateTopicAsync(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaBootstrapAddress }).Build();

        // One partition keeps events in publish order.
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
    }

    // The worker commits the offset only after its database transaction has committed or rolled back,
    // so a committed offset past the message means the app has finished with it. This lets a test
    // assert "nothing was saved" without sleeping.
    public async Task WaitForCommittedOffsetAsync(string consumerGroupId, TopicPartitionOffset delivered, TimeSpan? timeout = null)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaBootstrapAddress }).Build();
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(30));

        while (true)
        {
            var groups = await admin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(consumerGroupId, [delivered.TopicPartition])]);
            var committed = groups.Single().Partitions.Single().Offset;

            if (committed != Offset.Unset && committed.Value > delivered.Offset.Value)
                return;

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cts.Token);
            }
            catch (OperationCanceledException)
            {
                throw new TimeoutException(
                    $"Consumer group '{consumerGroupId}' did not commit past {delivered} (committed: {committed}).");
            }
        }
    }

    public async Task DisposeAsync()
    {
        await _kafka.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class IntegrationTestCollection : ICollectionFixture<ContainersFixture>
{
    public const string Name = "Kafka + Postgres";
}
