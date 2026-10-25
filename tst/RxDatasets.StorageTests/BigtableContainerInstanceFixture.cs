using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Grpc.Core;
using RxDatasets.StorageProvider.GoogleCloud;

namespace RxDatasets.StorageTests;

/// <summary>The Bigtable emulator, with the table <see cref="TableId"/> created as production must have it.</summary>
public class BigtableContainerInstanceFixture : IAsyncLifetime
{
    private const int BigtablePort = 8086;
    public const string ProjectId = "test-project";
    public const string InstanceId = "test-instance";
    public const string TableId = "rxdatasets";

    public IContainer Container { get; } = new ContainerBuilder()
        .WithImage("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators")
        .WithPortBinding(BigtablePort, true)
        .WithCommand("gcloud", "beta", "emulators", "bigtable", "start", $"--host-port=0.0.0.0:{BigtablePort}")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(BigtablePort))
        .Build();

    /// <summary>The value for <see cref="GoogleCloudConfig.BigtableEmulatorHost"/>.</summary>
    public string EmulatorHost => $"{Container.Hostname}:{Container.GetMappedPublicPort(BigtablePort)}";

    public async Task InitializeAsync()
    {
        await Container.StartAsync();
        BigtableTableAdminClient admin = new BigtableTableAdminClientBuilder
        {
            Endpoint = EmulatorHost,
            ChannelCredentials = ChannelCredentials.Insecure
        }.Build();
        var oneVersion = new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } };
        await admin.CreateTableAsync(new InstanceName(ProjectId, InstanceId), TableId, new Table
        {
            ColumnFamilies =
            {
                { BigtableRowStore.MetaFamily, oneVersion },
                { BigtableRowStore.PayloadFamily, oneVersion }
            }
        });
    }

    public Task DisposeAsync() => Container.DisposeAsync().AsTask();
}

/// <summary>
/// Test classes that each run a fake GCS server: not in parallel, as two of them under load drop connections.
/// </summary>
[CollectionDefinition(Name)]
public class FakeGcsCollection
{
    public const string Name = "FakeGcs";
}
