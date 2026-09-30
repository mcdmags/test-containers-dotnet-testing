# Kafka + Postgres integration tests with Testcontainers

A .NET 10 example of black-box integration tests for a Kafka consumer that writes to two Postgres tables in one transaction, including tests that inject failures through DI mocks and assert on logs.

Stack: Confluent.Kafka, EF Core (Npgsql), xUnit, Moq, `Microsoft.Extensions.Diagnostics.Testing` (FakeLogger), Testcontainers.

## The scenario

A background worker consumes `OrderPlacedEvent` messages from Kafka and saves each one to two tables, `orders` and `shipments`. Both writes must succeed or fail together, so they run in one database transaction.

The integration tests should treat the worker as a black box: publish an event to Kafka, then check the database. They never call the worker or its services directly.

## The problem

The happy path is easy to test. The hard part is proving the **all-or-nothing** behaviour. The natural approach:

1. Give the test fixture an `Action<IServiceCollection>` so a test can swap in its own registrations.
2. Replace a repository with a Moq mock whose `UpsertAsync` throws.
3. Publish an event, then assert that neither table has a row and that an error was logged (using a mocked `ILogger`).

That test passes when run on its own and **fails when the whole suite runs**: rows are saved, and the mocked logger is never called. Several separate things cause this:

- **The app host is built once and reused.** `WebApplicationFactory` builds the host on first access to `Services` and caches it. When the fixture is shared (`IClassFixture`/`ICollectionFixture`), whichever test runs first decides the registrations. A later test's `Action<IServiceCollection>` is silently ignored, so the real repository and the real logger are used.
- **The override runs too early.** With minimal hosting, `builder.ConfigureServices(...)` inside `ConfigureWebHost` runs *before* `Program.cs` registers its services, so the app's own registration wins over the mock.
- **Hosts compete for the same messages.** If more than one host runs a consumer on the same topic and consumer group, Kafka can deliver the test's event to a host that doesn't have the mock.
- **The test checks too early.** "Nothing was saved" can't be polled for, and the error is logged only after the rollback. Asserting straight after publishing is a race.
- **The mock can make the test meaningless.** If the mocked repository is the one that would write both tables, nothing is ever written, so the test passes even without a transaction.
- **Mocking `ILogger` is brittle.** It needs `It.IsAnyType` matching and an `IsEnabled` setup, and it breaks when the logging style changes (for example `[LoggerMessage]` source generation).

The app's production Kafka config also uses SASL authentication, while a test broker has none. So the test config must switch the protocol off SASL instead of supplying dummy credentials.

## The solution

| Problem | Approach |
|---|---|
| Per-test DI overrides ignored by a shared host | One `TestFixture` (a `WebApplicationFactory<Program>`) per test, created with `TestFixture.StartAsync(containers, services => ...)`. Only the containers are shared (`ContainersFixture`). |
| The app's registration wins over the mock | Apply overrides in `ConfigureTestServices`, which runs after `Program.cs`. |
| Another host consumes the test's event | Each `TestFixture` gets a unique topic and consumer group. |
| Knowing when the worker is done | The worker commits the Kafka offset only after the transaction commits or rolls back. `PublishAndWaitUntilProcessedAsync` waits for that commit, so no `Task.Delay`. |
| Proving the rollback | Mock only the *second* write (`IRepository<Shipment>`). The first write is real and runs inside the transaction, so an empty `orders` table proves it was rolled back. A second test causes a real constraint violation, with no mocks. |
| Asserting on logs | Every test host registers FakeLogger. Tests read `app.Logs` (a `FakeLogCollector`) instead of mocking `ILogger<T>`. |
| SASL in production, no auth in tests | SASL settings are applied only for SASL protocols. The test host sets `SecurityProtocol=Plaintext`. |

Example: the failure test.

```csharp
var shipmentRepository = new Mock<IRepository<Shipment>>();
shipmentRepository
    .Setup(r => r.UpsertAsync(It.IsAny<Shipment>(), It.IsAny<CancellationToken>()))
    .ThrowsAsync(new InvalidOperationException("Simulated shipment upsert failure"));

await using var app = await TestFixture.StartAsync(containers, services =>
{
    services.RemoveAll<IRepository<Shipment>>();
    services.AddScoped(_ => shipmentRepository.Object);
});

await app.PublishAndWaitUntilProcessedAsync(evt);

// Assert: error logged, and neither orders nor shipments has a row for evt.OrderId.
```

## Project layout

- `src/OrderProcessor`: the worker. `OrderPlacedConsumerWorker` consumes events. `OrderProcessorService` writes both tables in one transaction through `IRepository<T>` (EF Core).
- `tests/OrderProcessor.IntegrationTests`:
  - `ContainersFixture`: starts Kafka and Postgres once per test run.
  - `TestFixture`: a per-test app host that accepts DI overrides.
  - `OrderPlacedConsumerTests`: happy path, mocked failure, database failure, and redelivery (upsert).

## Run

Requires the .NET 10 SDK and Docker.

```sh
dotnet test
```

With colima, point Testcontainers at its socket first:

```sh
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

## License

[MIT](LICENSE)
