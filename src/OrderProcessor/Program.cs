using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderProcessor;
using OrderProcessor.Data;
using OrderProcessor.Kafka;
using OrderProcessor.Processing;

var builder = WebApplication.CreateBuilder(args);

// Read eagerly at startup. Integration tests override these with WebApplicationFactory's UseSetting,
// which is applied before this line runs.
var kafkaSettings = builder.Configuration.GetSection(KafkaSettings.SectionName).Get<KafkaSettings>()
    ?? throw new InvalidOperationException($"Missing '{KafkaSettings.SectionName}' configuration section.");
var topic = kafkaSettings.OrderPlacedTopic;

builder.Services.AddSingleton(kafkaSettings);

builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("OrdersDb")));
builder.Services.AddScoped(typeof(IRepository<>), typeof(EfRepository<>));
builder.Services.AddScoped<IOrderProcessorService, OrderProcessorService>();

builder.Services.AddKafkaProducer<string, OrderPlacedEvent>(kafkaSettings, topic);
builder.Services.AddKafkaConsumer<string, OrderPlacedEvent>(kafkaSettings, topic);
builder.Services.AddHostedService(sp => new OrderPlacedConsumerWorker(
    sp.GetRequiredKeyedService<IConsumer<string, OrderPlacedEvent>>(topic),
    sp.GetRequiredService<IServiceScopeFactory>(),
    topic,
    sp.GetRequiredService<ILogger<OrderPlacedConsumerWorker>>()));

var app = builder.Build();

app.MapGet("/health", () => Results.Ok("Healthy"));

app.Run();

public partial class Program;
