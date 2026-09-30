using Confluent.Kafka;

namespace OrderProcessor.Kafka;

public sealed class KafkaSettings
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "";
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.SaslSsl;
    public SaslMechanism SaslMechanism { get; set; } = SaslMechanism.Plain;
    public string? ApiKeyId { get; set; }
    public string? ApiKeySecret { get; set; }
    public string OrderPlacedTopic { get; set; } = "order-placed-events";
    public string ConsumerGroupId { get; set; } = "order-processor";

    // SASL settings are only applied when the protocol uses SASL, so a Plaintext broker
    // (e.g. a local or Testcontainers one) needs no credentials.
    public T Apply<T>(T config) where T : ClientConfig
    {
        config.BootstrapServers = BootstrapServers;
        config.SecurityProtocol = SecurityProtocol;

        if (SecurityProtocol is SecurityProtocol.SaslSsl or SecurityProtocol.SaslPlaintext)
        {
            config.SaslMechanism = SaslMechanism;
            config.SaslUsername = ApiKeyId;
            config.SaslPassword = ApiKeySecret;
        }

        return config;
    }
}
