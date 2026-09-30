using Confluent.Kafka;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Testing;

namespace OrderProcessor.IntegrationTests;

// Boots the app's Program.cs (WebApplicationBuilder) against the shared containers.
//
// Create ONE per test, never share it: WebApplicationFactory builds its host once, on first access to Services,
// and caches it. A shared instance ignores any later test's configureServices, so a mock registered by the
// second test never replaces the real registration.
public sealed class TestFixture : WebApplicationFactory<Program>
{
    private readonly ContainersFixture _containers;
    private readonly Action<IServiceCollection>? _configureServices;

    private TestFixture(ContainersFixture containers, Action<IServiceCollection>? configureServices)
    {
        _containers = containers;
        _configureServices = configureServices;
    }

    // Unique per host, so only this host's worker consumes this test's events. With a shared topic and group,
    // Kafka could hand the event to another test's host that doesn't have the mock.
    public string Topic { get; } = $"order-placed-events-{Guid.NewGuid():N}";
    public string ConsumerGroupId { get; } = $"order-processor-{Guid.NewGuid():N}";

    // Every log record the app writes, captured in memory by FakeLogger.
    public FakeLogCollector Logs => Services.GetFakeLogCollector();

    public static async Task<TestFixture> StartAsync(
        ContainersFixture containers, Action<IServiceCollection>? configureServices = null)
    {
        var fixture = new TestFixture(containers, configureServices);
        await containers.CreateTopicAsync(fixture.Topic);

        // Builds and starts the host: Program.cs registrations, then the test overrides, then the consumer worker.
        _ = fixture.Services;
        return fixture;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        // The test broker has no authentication, so switch off SASL instead of supplying credentials.
        builder.UseSetting("ConnectionStrings:OrdersDb", _containers.PostgresConnectionString);
        builder.UseSetting("Kafka:BootstrapServers", _containers.KafkaBootstrapAddress);
        builder.UseSetting("Kafka:SecurityProtocol", nameof(SecurityProtocol.Plaintext));
        builder.UseSetting("Kafka:OrderPlacedTopic", Topic);
        builder.UseSetting("Kafka:ConsumerGroupId", ConsumerGroupId);

        // ConfigureTestServices runs AFTER Program.cs has registered its services, so these replace them.
        // (ConfigureServices here runs BEFORE Program.cs, and the app's own registration would win.)
        builder.ConfigureTestServices(services =>
        {
            services.AddFakeLogging();
            _configureServices?.Invoke(services);
        });
    }

    // Publishes through the app's own DI-registered producer, like any other client of the topic would.
    public Task<DeliveryResult<string, OrderPlacedEvent>> PublishAsync(OrderPlacedEvent evt) =>
        Services.GetRequiredKeyedService<IProducer<string, OrderPlacedEvent>>(Topic)
            .ProduceAsync(Topic, new Message<string, OrderPlacedEvent> { Key = evt.OrderId, Value = evt });

    // Returns once the worker has finished the message: transaction committed or rolled back, logs written.
    public async Task PublishAndWaitUntilProcessedAsync(OrderPlacedEvent evt)
    {
        var delivered = await PublishAsync(evt);
        await _containers.WaitForCommittedOffsetAsync(ConsumerGroupId, delivered.TopicPartitionOffset);
    }
}
