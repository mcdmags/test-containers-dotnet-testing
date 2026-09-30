using System.Text.Json;
using Confluent.Kafka;

namespace OrderProcessor.Kafka;

public sealed class JsonSerde<T> : ISerializer<T>, IDeserializer<T>
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public byte[] Serialize(T data, SerializationContext context) =>
        JsonSerializer.SerializeToUtf8Bytes(data, Options);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        JsonSerializer.Deserialize<T>(data, Options)
        ?? throw new InvalidDataException($"Kafka message could not be deserialized to {typeof(T).Name}.");
}
