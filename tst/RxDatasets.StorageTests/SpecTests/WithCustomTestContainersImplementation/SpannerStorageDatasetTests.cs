using System.Data;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.Spanner;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

/// <summary>All datasets in one set of tables; each test is its own dataset.</summary>
[Collection(SpannerEmulatorFixture.Name)]
public class SpannerStorageDatasetTests(SpannerEmulatorFixture spanner) : StorageTestSpecWithTwoStepInit
{
    protected virtual bool TablePerDataset => false;

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        var provider = new SpannerStorageProvider<object>(Mock.Of<ILogger<SpannerStorageProvider<object>>>(),
            await CreateStore(memberName), notificationsHub, startIfInitFails: false);
        await provider.StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), provider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), provider,
            Options.Create(new WriterConfig()), false);

        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(Mock.Of<ILogger<SnapshotCreationPlugin<object>>>(),
            provider, SnapshotsOptions, new TestScheduler());
        _ = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(
            Mock.Of<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>(), notificationsHub,
            snapshotCreationPlugin).StartAsync(CancellationToken.None);
        return (datasetReader, datasetWriter);
    }

    private async Task<SpannerDatasetStore<object>> CreateStore(string memberName)
    {
        // Short enough for a table name when TablePerDataset is set
        StorageConfig.Value.DtoName = new string(memberName.TakeLast(60).ToArray());
        var config = Options.Create(new SpannerConfig
        {
            ConnectionString = await spanner.ConnectionString(),
            TablePerDataset = TablePerDataset
        });
        return new SpannerDatasetStore<object>(config, StorageConfig);
    }

    [Fact]
    public async Task Should_Only_Store_The_Delta_Version_After_The_Latest_Even_After_Cleaning()
    {
        var store = await CreateStore(nameof(Should_Only_Store_The_Delta_Version_After_The_Latest_Even_After_Cleaning));
        for (ulong version = 1; version <= 3; version++)
        {
            (await store.StoreDelta(version, [1], Stream.Null)).Should().BeTrue();
        }

        (await store.StoreDelta(5, [1], Stream.Null)).Should().BeFalse("versions have no gaps");
        (await store.StoreSnapshot(3, [1], Stream.Null)).Should().BeTrue();
        await store.DeleteOlderThan(3, keepAllDeltas: false);

        var (oldest, newest, snapshots) = await store.ListVersions();
        (oldest, newest).Should().Be((4ul, 3ul));
        snapshots.Select(s => s.Version).Should().Equal(3ul);
        (await store.StoreDelta(2, [1], Stream.Null)).Should().BeFalse("cleaning removed it, but it is not after the latest");
        (await store.StoreDelta(3, [1], Stream.Null)).Should().BeFalse();
        (await store.StoreDelta(4, [1], Stream.Null)).Should().BeTrue();
        (await store.ListVersions()).Should().BeEquivalentTo((4ul, 4ul, snapshots));
    }

    [Fact]
    public async Task Should_Reject_A_Version_Another_Writer_Stored_And_Continue_After_Reloading()
    {
        var (_, first) = await SetupStorageAsync(CancellationToken.None);
        var (_, second) = await SetupStorageAsync(CancellationToken.None);

        (await first.Put("from_first", GenerateRandomData(), null, null)).NewDatasetVersion.Should().Be(1);
        var stale = () => second.Put("from_second", GenerateRandomData(seed: 1), null, null);
        await stale.Should().ThrowAsync<DBConcurrencyException>();

        // The failed write reloaded the second writer
        (await second.Put("from_second", GenerateRandomData(seed: 1), null, null)).NewDatasetVersion.Should().Be(2);
        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        (await newReader.Get("from_first")).Should().NotBeNull();
        (await newReader.Get("from_second")).Should().NotBeNull();
    }

    [Fact]
    public async Task Should_Store_And_Read_Entities_Larger_Than_A_Chunk_And_A_Commit()
    {
        var (reader, writer) = await SetupStorageAsync(CancellationToken.None);
        var small = GenerateRandomData(UNIT_KB);
        var large = GenerateRandomData(40 * 1024 * UNIT_KB, seed: 1);

        await writer.PutMany(new[] { ("small", small), ("large", large), ("after", small) }
            .Select(e => (e.Item1, e.Item2, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable());

        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        foreach (var r in new[] { reader, newReader })
        {
            (await r.Get("large"))!.Value.entityBytes.ToArray().Should().Equal(large);
            (await r.Get("after"))!.Value.entityBytes.ToArray().Should().Equal(small);
        }
    }
}

/// <summary>The same suite with a set of tables per dataset.</summary>
[Collection(SpannerEmulatorFixture.Name)]
public class SpannerTablePerDatasetTests(SpannerEmulatorFixture spanner) : SpannerStorageDatasetTests(spanner)
{
    protected override bool TablePerDataset => true;
}
