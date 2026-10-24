using System.Collections.Concurrent;
using FluentAssertions;
using Google.Cloud.Spanner.Data;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProvider.Spanner;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

/// <summary>Spanner provider behavior beyond the storage spec: start-up, leadership, cleaning, wiring.</summary>
[Collection(SpannerEmulatorFixture.Name)]
public class SpannerProviderTests(SpannerEmulatorFixture spanner) : IAsyncLifetime
{
    private readonly List<SpannerStorageProvider<object>> _providers = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var provider in _providers)
        {
            await provider.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Should_Send_Ready_Only_Once_The_Versions_Are_Listed_When_Starting_Despite_Failures()
    {
        // The database doesn't exist yet: the first listings fail
        var (provider, hub) = Provider(SpannerEmulatorFixture.ConnectionString("late"), "late", startIfInitFails: true);
        var readiness = Record(hub);
        var writer = new DatasetWriter<object>(hub, Mock.Of<ILogger<DatasetWriter<object>>>(), provider,
            Options.Create(new WriterConfig()));
        await provider.StartAsync(CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(2));
        readiness.Should().BeEmpty();

        await SpannerEmulatorFixture.CreateDatabase("late");
        await WaitUntil(() => !readiness.IsEmpty, TimeSpan.FromSeconds(20));
        readiness.Should().Equal(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, IsLeader: true));
        await WaitUntil(() => writer.InitializationTask is { IsCompletedSuccessfully: true }, TimeSpan.FromSeconds(10));
        (await writer.Put("entity", [1, 2, 3], null, null)).NewDatasetVersion.Should().Be(1);
    }

    [Fact]
    public async Task Should_Elect_One_Leader_And_Hand_Over_When_It_Stops()
    {
        var connectionString = await spanner.ConnectionString();
        var (first, firstHub) = Provider(connectionString, "leadership");
        var (second, secondHub) = Provider(connectionString, "leadership");
        var firstReadiness = Record(firstHub);
        var secondReadiness = Record(secondHub);
        await first.StartAsync(CancellationToken.None);
        await second.StartAsync(CancellationToken.None);

        firstReadiness.Should().Equal(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, IsLeader: true));
        secondReadiness.Should().Equal(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, IsLeader: false));

        await first.StopAsync(CancellationToken.None);
        await WaitUntil(() => secondReadiness.Count == 2, TimeSpan.FromSeconds(15));
        secondReadiness.Last().Should().Be(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, IsLeader: true));
        await second.StopAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData(true, new ulong[] { 2, 3 }, 3ul)]
    [InlineData(false, new ulong[] { 1, 2, 3 }, 1ul)]
    public async Task Should_Clean_On_The_Leader_Only(bool leader, ulong[] snapshotsLeft, ulong oldestDeltaLeft)
    {
        var connectionString = await spanner.ConnectionString();
        var dataset = $"cleaning{leader}";
        if (!leader)
        {
            var (other, _) = Provider(connectionString, dataset);
            await other.StartAsync(CancellationToken.None);
        }

        var (provider, hub) = Provider(connectionString, dataset);
        await provider.StartAsync(CancellationToken.None);
        for (ulong version = 1; version <= 3; version++)
        {
            await provider.StoreDelta(version, [1], []);
            await provider.StoreSnapshot(version, [1], []);
        }

        await provider.StoreProcessedVersion("reporter", 3);
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTimeOffset.UtcNow.AddHours(3).UtcTicks);
        await new StorageCleaningService<object>(hub, provider, scheduler,
                Options.Create(new CleanupConfig { KeepAllDeltas = false }), Mock.Of<ILogger<StorageCleaningService<object>>>())
            .StartAsync(CancellationToken.None);

        await WaitUntil(async () => (await provider.ListVersions()).OldestDelta != 1, TimeSpan.FromSeconds(2));
        var (oldest, newest, snapshots) = await provider.ListVersions();
        snapshots.Select(s => s.Version).Should().Equal(snapshotsLeft);
        (oldest, newest).Should().Be((oldestDeltaLeft, 3ul));
    }

    [Fact]
    public async Task Should_Remove_Chunks_That_No_Version_Points_At_After_A_Day()
    {
        var connectionString = await spanner.ConnectionString();
        var (provider, _) = Provider(connectionString, "orphans");
        await provider.StartAsync(CancellationToken.None);
        await provider.StoreDelta(1, [1], [2]);
        await provider.StoreSnapshot(1, [1], [2]);
        await InsertChunk(connectionString, "orphans/", "old", DateTime.UtcNow.AddDays(-2));
        await InsertChunk(connectionString, "orphans/", "new", DateTime.UtcNow.AddMinutes(-5));

        await provider.DeleteOlderThan(1, keepAllDeltas: true);

        (await ChunkBlobIds(connectionString, "orphans/")).Should().Contain("new").And.NotContain("old").And.HaveCount(5);
        (await provider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, 1, 0)], 1)).ToArray().Should().Equal(2);
    }

    [Fact]
    public async Task Should_Wire_Read_Write_And_Read_Only_Hosts_With_Cache()
    {
        var connectionString = await spanner.ConnectionString();
        using var writerHost = Host(connectionString, readOnly: false);
        var writer = writerHost.Services.GetRequiredService<IDatasetWriter<StringValue>>();
        var reader = writerHost.Services.GetRequiredService<IDatasetReader<StringValue>>();
        await writerHost.StartAsync();
        using var readerHost = Host(connectionString, readOnly: true);
        var readOnlyReader = readerHost.Services.GetRequiredService<IDatasetReader<StringValue>>();
        await readerHost.StartAsync();

        await WaitUntil(async () =>
        {
            try
            {
                await writer.Put("entity", new StringValue { Value = "hello" }.ToByteArray(), null, null);
                return true;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(20));

        // Both readers catch up in the background: the read/write one on the write's notification, the read-only one
        // on its next poll
        await WaitUntil(async () => await reader.Get("entity") is not null, TimeSpan.FromSeconds(5));
        (await reader.Get("entity")).Should().NotBeNull();
        await WaitUntil(async () => await readOnlyReader.Get("entity") is not null, TimeSpan.FromSeconds(20));
        StringValue.Parser.ParseFrom((await readOnlyReader.Get("entity"))!.Value.entityBytes.Span).Value.Should().Be("hello");

        await readerHost.StopAsync();
        await writerHost.StopAsync();
    }

    private static IHost Host(string connectionString, bool readOnly)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Spanner:ConnectionString"] = connectionString,
            ["Spanner:CreateTables"] = "true",
            ["Storage:DtoName"] = "wiring"
        });
        var server = readOnly
            ? builder.Services.AddReadOnlyDtoServer<StringValue>()
            : builder.Services.AddDtoServer<StringValue>();
        server = server.WithSpannerStorage().WithCache();
        if (!readOnly)
        {
            server.WithSnapshot().WithCleanup();
        }

        server.Build();
        return builder.Build();
    }

    private (SpannerStorageProvider<object> Provider, INotificationsHub<object> Hub) Provider(
        string connectionString, string dataset, bool startIfInitFails = false)
    {
        var config = Options.Create(new SpannerConfig { ConnectionString = connectionString, CreateTables = true });
        var storageConfig = Options.Create(new StorageConfig { DtoName = dataset });
        var hub = new NotificationsHubSync<object>();
        var provider = new SpannerStorageProvider<object>(Mock.Of<ILogger<SpannerStorageProvider<object>>>(),
            new SpannerDatasetStore<object>(config, storageConfig), hub, startIfInitFails);
        _providers.Add(provider);
        return (provider, hub);
    }

    private static ConcurrentQueue<StorageProviderReadinessNotifcation> Record(INotificationsHub<object> hub)
    {
        var notifications = new ConcurrentQueue<StorageProviderReadinessNotifcation>();
        hub.Subscribe<StorageProviderReadinessNotifcation>(n =>
        {
            notifications.Enqueue(n);
            return Task.CompletedTask;
        });
        return notifications;
    }

    private static Task WaitUntil(Func<bool> condition, TimeSpan timeout) =>
        WaitUntil(() => Task.FromResult(condition()), timeout);

    private static async Task WaitUntil(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(200);
        }
    }

    private static async Task InsertChunk(string connectionString, string dataset, string blobId, DateTime createdAt)
    {
        await using var connection = new SpannerConnection(connectionString);
        await using var command = connection.CreateInsertCommand("RxDatasets_Chunks", new SpannerParameterCollection
        {
            { "Dataset", SpannerDbType.String, dataset },
            { "BlobId", SpannerDbType.String, blobId },
            { "Part", SpannerDbType.Int64, 1L },
            { "ChunkNo", SpannerDbType.Int64, 0L },
            { "Data", SpannerDbType.Bytes, new byte[] { 9 } },
            { "CreatedAt", SpannerDbType.Timestamp, createdAt }
        });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> ChunkBlobIds(string connectionString, string dataset)
    {
        await using var connection = new SpannerConnection(connectionString);
        await using var command = connection.CreateSelectCommand(
            "SELECT BlobId FROM RxDatasets_Chunks WHERE Dataset = @dataset",
            new SpannerParameterCollection { { "dataset", SpannerDbType.String, dataset } });
        var blobIds = new List<string>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            blobIds.Add(reader.GetString(0));
        }

        return blobIds;
    }
}
