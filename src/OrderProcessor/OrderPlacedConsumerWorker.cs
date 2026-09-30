using Confluent.Kafka;
using OrderProcessor.Processing;

namespace OrderProcessor;

public sealed class OrderPlacedConsumerWorker(
    IConsumer<string, OrderPlacedEvent> consumer,
    IServiceScopeFactory scopeFactory,
    string topic,
    ILogger<OrderPlacedConsumerWorker> logger) : BackgroundService
{
    // Consume() blocks, so run the loop on its own thread and let host startup continue.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => ConsumeLoop(stoppingToken), stoppingToken,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task ConsumeLoop(CancellationToken stoppingToken)
    {
        consumer.Subscribe(topic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, OrderPlacedEvent> result;
                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Failed to consume from {Topic}", topic);
                    continue;
                }

                try
                {
                    // New scope per message: fresh DbContext, and scoped registrations (including test overrides)
                    // are resolved here rather than captured once at startup.
                    await using var scope = scopeFactory.CreateAsyncScope();
                    var processor = scope.ServiceProvider.GetRequiredService<IOrderProcessorService>();
                    await processor.ProcessAsync(result.Message.Value, stoppingToken);

                    logger.LogInformation("Saved order {OrderId} from {Offset}",
                        result.Message.Value.OrderId, result.TopicPartitionOffset);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Transaction was rolled back, nothing was written. In production route this to a DLQ;
                    // here the message is skipped so one bad event does not block the partition.
                    logger.LogError(ex, "Rolled back order {OrderId} from {Offset}",
                        result.Message.Value.OrderId, result.TopicPartitionOffset);
                }

                // Committed only after the database transaction has finished (committed or rolled back).
                consumer.Commit(result);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }
}
