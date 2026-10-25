using System.Runtime.CompilerServices;
using FluentAssertions;
using Google.Protobuf;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

/// <summary>
/// The storage spec against the Bigtable emulator, with the fake GCS server as the bucket. Payloads over 4 KB go to the
/// bucket, so both inline and offloaded payloads are covered.
/// </summary>
[Collection(FakeGcsCollection.Name)]
public class BigtableDatasetTests(GoogleCloudContainerInstanceFixture gcs, BigtableContainerInstanceFixture bigtable)
    : StorageTestSpecWithTwoStepInit, IClassFixture<GoogleCloudContainerInstanceFixture>,
        IClassFixture<BigtableContainerInstanceFixture>, IAsyncLifetime
{
    private const int InlineLimit = 4 * 1024;

    private readonly IOptions<GoogleCloudConfig> _config = Options.Create(new GoogleCloudConfig
    {
        ProjectId = BigtableContainerInstanceFixture.ProjectId,
        BigtableInstanceId = BigtableContainerInstanceFixture.InstanceId,
        BigtableTableId = BigtableContainerInstanceFixture.TableId,
        MaxInlinePayloadBytes = InlineLimit
    });

    public async Task InitializeAsync()
    {
        if (gcs.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await gcs.Container.StartAsync();

        _config.Value.EmulatorHost = gcs.GetConnectionString().Replace("http://", "").TrimEnd('/');
        _config.Value.BigtableEmulatorHost = bigtable.EmulatorHost;
    }

    public Task DisposeAsync() => Task.CompletedTask;

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct, [CallerMemberName] string memberName = "")
    {
        var (reader, writer, _) = await Setup(memberName);
        return (reader, writer);
    }

    private async Task<(DatasetReader<object>, DatasetWriter<object>, BigtableStorageProvider<object>)> Setup(string dataset)
    {
        StorageConfig.Value.DtoName = dataset;
        GoogleCloudStorage<object> bucket = await Bucket(dataset);
        var rowStore = new BigtableRowStore(_config);
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        var provider = new BigtableStorageProvider<object>(Mock.Of<ILogger<BigtableStorageProvider<object>>>(),
            StorageConfig, _config, bucket, rowStore, notificationsHub,
            new BigtableStorageReader<object>(StorageConfig, _config, rowStore, bucket), false);
        await provider.StartAsync(CancellationToken.None);

        var reader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), provider, notificationsHub);
        await reader.StartAsync(CancellationToken.None);
        var writer = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), provider,
            Options.Create(new WriterConfig()), false);

        var snapshotPlugin = new SnapshotCreationPlugin<object>(Mock.Of<ILogger<SnapshotCreationPlugin<object>>>(), provider,
            SnapshotsOptions, new TestScheduler());
        _ = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(
            Mock.Of<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>(), notificationsHub,
            snapshotPlugin).StartAsync(CancellationToken.None);
        return (reader, writer, provider);
    }

    /// <summary>A bucket per test (and a dataset, a row key prefix, per test); the table is shared.</summary>
    private async Task<GoogleCloudStorage<object>> Bucket(string name)
    {
        _config.Value.BucketName = new string(name.ToLower().Replace("_", "").TakeLast(63).ToArray());
        var client = new Google.Cloud.Storage.V1.StorageClientBuilder
        {
            BaseUri = $"http://{_config.Value.EmulatorHost}/storage/v1/",
            UnauthenticatedAccess = true
        }.Build();
        try
        {
            await client.CreateBucketAsync("test-project",
                new Google.Apis.Storage.v1.Data.Bucket { Name = _config.Value.BucketName });
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Conflict)
        {
        }

        return new GoogleCloudStorage<object>(Mock.Of<ILogger<GoogleCloudStorage<object>>>(), _config,
            retryInitialization: false, enableWriteQueue: false);
    }

    private static Task<List<(string ObjectKey, DateTimeOffset DateTime)>> Objects(IDiskStorage<object> bucket,
        string dataset, string folder) => bucket.ListObjects($"{dataset}/{folder}/");

    [Fact]
    public async Task Should_Reject_A_Delta_Of_A_Stale_Writer_And_Reload()
    {
        const string dataset = nameof(Should_Reject_A_Delta_Of_A_Stale_Writer_And_Reload);
        var (_, writerA, _) = await Setup(dataset);
        var (_, writerB, _) = await Setup(dataset);
        byte[] large = GenerateRandomData(10 * UNIT_KB, 1); // over the inline limit: the payloads go to the bucket

        (await writerA.Put("a", large, null, null)).NewDatasetVersion.Should().Be(1);

        // B still thinks the dataset is at version 0, so it writes version 1 again
        var stale = () => writerB.Put("b", GenerateRandomData(10 * UNIT_KB, 2), null, null);
        await stale.Should().ThrowAsync<InvalidOperationException>();
        // ...and removed the payload it uploaded, leaving A's
        (await Objects(await Bucket(dataset), dataset, "deltas/1")).Should().ContainSingle();

        // ...after which B reloaded, and writes on top of A's delta
        (await writerB.Put("b", GenerateRandomData(10 * UNIT_KB, 2), null, null)).NewDatasetVersion.Should().Be(2);
        var (reader, _, _) = await Setup(dataset);
        (await reader.Get("a"))!.Value.entityBytes.ToArray().Should().Equal(large);
        (await reader.Get("b")).Should().NotBeNull();
    }

    [Fact]
    public async Task Should_Reject_A_Cleaned_Up_Version_Of_A_Far_Behind_Writer()
    {
        const string dataset = nameof(Should_Reject_A_Cleaned_Up_Version_Of_A_Far_Behind_Writer);
        var (_, writerA, providerA) = await Setup(dataset);
        var (_, writerB, _) = await Setup(dataset);
        await writerA.Put("a1", GenerateRandomData(UNIT_KB, 1), null, null);
        await writerA.Put("a2", GenerateRandomData(UNIT_KB, 2), null, null);
        await providerA.StoreSnapshot(2, new SnapshotIndex { SnapshotVersion = 2 }.ToByteArray(), Array.Empty<byte>());
        await providerA.DeleteOlderThan(2, keepAllDeltas: false); // delta 1 is gone

        // B is still at version 0: its version 1 has no row any more, but the snapshot is past it
        var stale = () => writerB.Put("b", GenerateRandomData(UNIT_KB, 3), null, null);
        await stale.Should().ThrowAsync<InvalidOperationException>();
        await ((Func<Task>)(() => providerA.ReadDeltaIndex(1))).Should().ThrowAsync<FileNotFoundException>();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Should_Clean_Up_Older_Snapshots_And_Deltas_But_The_Newest(bool keepAllDeltas)
    {
        string dataset = $"{nameof(Should_Clean_Up_Older_Snapshots_And_Deltas_But_The_Newest)}_{keepAllDeltas}";
        var (_, writer, provider) = await Setup(dataset);
        for (var i = 1; i <= 3; i++)
            await writer.Put($"e{i}", GenerateRandomData(10 * UNIT_KB, i), null, null); // data in the bucket
        await provider.StoreSnapshot(2, new SnapshotIndex { SnapshotVersion = 2 }.ToByteArray(), Array.Empty<byte>());
        await provider.StoreSnapshot(3, new SnapshotIndex { SnapshotVersion = 3 }.ToByteArray(), Array.Empty<byte>());

        // A payload of a write that never got its row
        await (await Bucket(dataset)).UploadBytes($"{dataset}/deltas/1/orphan-data.pbs", [1, 2, 3]);

        await provider.DeleteOlderThan(3, keepAllDeltas);

        var (_, _, provider2) = await Setup(dataset);
        var (oldest, newest, snapshots) = await provider2.ListVersions();
        (oldest, newest).Should().Be((keepAllDeltas ? 1ul : 3ul, 3ul));
        snapshots.Select(s => s.Version).Should().Equal(3ul);
        DeltaIndex.Parser.ParseFrom((await provider2.ReadDeltaIndex(3)).Span).DeltaVersion.Should().Be(3);
        var deltaObjects = await Objects(await Bucket(dataset), dataset, "deltas");
        deltaObjects.Should().HaveCount(keepAllDeltas ? 4 : 1);
        if (!keepAllDeltas)
            await ((Func<Task>)(() => provider2.ReadDeltaIndex(2))).Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task Should_Read_Back_Compressed_Payloads()
    {
        _config.Value.CompressPayloads = true;
        _config.Value.MaxInlinePayloadBytes = 1024 * 1024;
        const string dataset = nameof(Should_Read_Back_Compressed_Payloads);
        var (_, writer, _) = await Setup(dataset);
        byte[] compressible = Enumerable.Repeat((byte)7, 3 * (int)UNIT_KB).ToArray();
        var entities = Enumerable.Range(0, 20)
            .Select(i => ($"e/{i}", compressible.Append((byte)i).ToArray(), (string?)null, (DateTimeOffset?)null))
            .ToList();
        await writer.PutMany(entities.ToAsyncEnumerable()); // 60 KB of data: compressed, in the row

        BigtableArtifact? row = await new BigtableRowStore(_config).Read($"{dataset}/", ObjectNameTypeEnum.Delta, 1,
            BigtablePayload.Data);
        row!.DataEncoding.Should().Be(BigtableRowStore.Brotli);
        row.Data!.Length.Should().BeLessThan(4 * (int)UNIT_KB);
        var (reader, _, _) = await Setup(dataset);
        foreach (var (name, bytes, _, _) in entities)
            (await reader.Get(name))!.Value.entityBytes.ToArray().Should().Equal(bytes);
    }

    [Fact]
    public async Task Should_Serve_A_Read_Only_Host_And_Report_Health_Through_DI()
    {
        string bucketName = nameof(Should_Serve_A_Read_Only_Host_And_Report_Health_Through_DI).ToLower().Replace("_", "");
        await Bucket(bucketName); // creates the bucket
        var settings = new Dictionary<string, string?>
        {
            ["GoogleCloud:BucketName"] = bucketName,
            ["GoogleCloud:ProjectId"] = BigtableContainerInstanceFixture.ProjectId,
            ["GoogleCloud:EmulatorHost"] = _config.Value.EmulatorHost,
            ["GoogleCloud:BigtableInstanceId"] = BigtableContainerInstanceFixture.InstanceId,
            ["GoogleCloud:BigtableTableId"] = BigtableContainerInstanceFixture.TableId,
            ["GoogleCloud:BigtableEmulatorHost"] = bigtable.EmulatorHost,
            ["Storage:DtoName"] = "HostDataset"
        };
        IHost Host(bool readOnly)
        {
            HostApplicationBuilder builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Configuration.AddInMemoryCollection(settings);
            var server = readOnly ? builder.Services.AddReadOnlyDtoServer<Demodto>() : builder.Services.AddDtoServer<Demodto>();
            server.WithGoogleBigtableStorage().Build();
            return builder.Build();
        }

        using IHost writeHost = Host(readOnly: false);
        var writer = writeHost.Services.GetRequiredService<IDatasetWriter<Demodto>>();
        await writeHost.StartAsync();
        await Eventually(() => writer.Put("first", new Demodto { Name = "first" }.ToByteArray(), null, null));

        using IHost readHost = Host(readOnly: true);
        var reader = readHost.Services.GetRequiredService<IDatasetReader<Demodto>>();
        await readHost.StartAsync();
        await Eventually(async () => (await reader.Get("first")).Should().NotBeNull());

        // A write after the read-only host started reaches it through its ListVersions polling
        await writer.Put("second", new Demodto { Name = "second" }.ToByteArray(), null, null);
        await Eventually(async () => (await reader.Get("second")).Should().NotBeNull(), TimeSpan.FromSeconds(20));

        foreach (IHost host in new[] { writeHost, readHost })
        {
            HealthReport report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
            report.Entries.Should().ContainKey($"storage.bigtable.{nameof(Demodto)}")
                .WhoseValue.Status.Should().Be(HealthStatus.Healthy);
        }

        await readHost.StopAsync();
        await writeHost.StopAsync();
    }

    private static async Task Eventually(Func<Task> action, TimeSpan? timeout = null)
    {
        DateTime until = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (true)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception) when (DateTime.UtcNow < until)
            {
                await Task.Delay(200);
            }
        }
    }
}
