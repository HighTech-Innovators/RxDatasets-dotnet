using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Api.Gax;
using Google.Cloud.Spanner.Admin.Instance.V1;
using Google.Cloud.Spanner.Data;

namespace RxDatasets.StorageTests;

/// <summary>
/// One emulator for every Spanner test class: the client finds it through a process-wide environment variable, so
/// classes running in parallel with their own emulators would talk to each other's.
/// </summary>
[CollectionDefinition(Name)]
public class SpannerEmulatorFixture : IAsyncLifetime, ICollectionFixture<SpannerEmulatorFixture>
{
    public const string Name = "Spanner emulator";
    private const string Instance = "projects/test-project/instances/test-instance";
    private const int GrpcPort = 9010;

    private readonly IContainer _container = new ContainerBuilder()
        .WithImage("gcr.io/cloud-spanner-emulator/emulator:1.5.51")
        .WithPortBinding(GrpcPort, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("gRPC server listening"))
        .Build();

    private Task<string>? _database;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        Environment.SetEnvironmentVariable("SPANNER_EMULATOR_HOST",
            $"{_container.Hostname}:{_container.GetMappedPublicPort(GrpcPort)}");

        var instanceAdmin = await new InstanceAdminClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }
            .BuildAsync();
        var creation = await instanceAdmin.CreateInstanceAsync(new CreateInstanceRequest
        {
            Parent = "projects/test-project",
            InstanceId = "test-instance",
            Instance = new Instance
            {
                Config = "projects/test-project/instanceConfigs/emulator-config", DisplayName = "test", NodeCount = 1
            }
        });
        await creation.PollUntilCompletedAsync();
    }

    /// <summary>One database for all tests; the provider creates its tables on first use.</summary>
    public Task<string> ConnectionString() => _database ??= CreateDatabase("rxdatasets");

    public static string ConnectionString(string database) =>
        $"Data Source={Instance}/databases/{database};EmulatorDetection=EmulatorOnly";

    public static async Task<string> CreateDatabase(string name)
    {
        await using var connection = new SpannerConnection($"Data Source={Instance};EmulatorDetection=EmulatorOnly");
        await using var command = connection.CreateDdlCommand($"CREATE DATABASE {name}");
        await command.ExecuteNonQueryAsync();
        return ConnectionString(name);
    }

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}
