using Confluent.Kafka;

namespace OrderProcessor.Kafka;

public static class KafkaServiceCollectionExtensions
{
    // Clients are keyed by topic name, so an app can hold several producers/consumers side by side.
    public static IServiceCollection AddKafkaProducer<TKey, TValue>(
        this IServiceCollection services, KafkaSettings settings, string topic) =>
        services.AddKeyedSingleton<IProducer<TKey, TValue>>(topic, (_, _) =>
            new ProducerBuilder<TKey, TValue>(settings.Apply(new ProducerConfig { Acks = Acks.All }))
                .SetValueSerializer(new JsonSerde<TValue>())
                .Build());

    public static IServiceCollection AddKafkaConsumer<TKey, TValue>(
        this IServiceCollection services, KafkaSettings settings, string topic) =>
        services.AddKeyedSingleton<IConsumer<TKey, TValue>>(topic, (_, _) =>
            new ConsumerBuilder<TKey, TValue>(settings.Apply(new ConsumerConfig
                {
                    GroupId = settings.ConsumerGroupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    // Offsets are committed by the worker only after the database transaction has finished.
                    EnableAutoCommit = false,
                }))
                .SetValueDeserializer(new JsonSerde<TValue>())
                .Build());
}
