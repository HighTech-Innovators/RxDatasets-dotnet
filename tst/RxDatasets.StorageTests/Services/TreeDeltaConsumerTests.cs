using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using FluentAssertions;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.DatasetWatcher;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

/// <summary>The consumers that read delta indexes directly, fed with tree-format deltas from the real writer.</summary>
public class TreeDeltaConsumerTests
{
    private const string Changed = "t/T/s/S/items/a/values";
    private const string Deleted = "t/T/s/S/items/b/values";
    private const string NewPage = "t/T/s/S/esls/B/pages/0/studioimage";
    private const string NewRoot = "esltypes/E";

    private readonly NotificationsHubSync<object> _notificationsHub = new();
    private readonly IStorageReader<object> _storageReader;
    private readonly ObjectStorageProvider<object> _storageProvider;
    private readonly DatasetWriter<object> _writer;

    public TreeDeltaConsumerTests()
    {
        var storage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var storageConfig = Options.Create(new StorageConfig());
        _storageReader = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), storageConfig, storage);
        _storageProvider = new ObjectStorageProvider<object>(Mock.Of<ILogger<ObjectStorageProvider<object>>>(), storageConfig,
            storage, _notificationsHub, _storageReader, false);
        _writer = new DatasetWriter<object>(_notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), _storageProvider,
            Options.Create(new WriterConfig()), false);
    }

    /// <summary>Writes versions 2 to 4 on top of version 1, and checks they are stored in the tree format.</summary>
    private async Task WriteNewChangedAndDeleted()
    {
        await _writer.PutMany(new[] { Deleted, NewPage, NewRoot }
            .Select(name => (name, new byte[] { 1 }, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable());
        await _writer.Put(Changed, [2], null, null);
        await _writer.Delete(Deleted, null, null);

        var delta = DeltaIndex.Parser.ParseFrom((await _storageReader.ReadDeltaIndex(2)).Span);
        delta.Root.Should().NotBeNull();
        delta.HasFlatEntries().Should().BeFalse();
    }

    [Fact]
    public async Task DatasetWatcher_ReportsTheFullNamesOfTreeDeltaEntries()
    {
        await _storageProvider.StartAsync(CancellationToken.None);
        var snapshotCreation = UsingLowLevelInMemoryStorage.CreateSnapshotCreationService(_storageProvider, _notificationsHub);
        await snapshotCreation.StartAsync(CancellationToken.None);
        await _writer.Put(Changed, [1], null, null);
        // The watcher starts from a snapshot; the first one is made for version 1, the next not before version 6
        var timeout = DateTime.UtcNow.AddSeconds(10);
        while ((await _storageReader.ListVersions()).Snapshots.Count == 0 && DateTime.UtcNow < timeout)
            await Task.Delay(10);
        await snapshotCreation.StopAsync(CancellationToken.None);
        await WriteNewChangedAndDeleted();

        var observer = new Mock<IDatasetObserver<object>>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        observer.Setup(o => o.OnDatasetVersionEnd(4)).Callback(() => cts.Cancel());
        var watcher = new DatasetWatcher<object>(Mock.Of<ILogger<DatasetWatcher<object>>>(), _storageReader,
            Options.Create(new DatasetWatcherConfig { CheckingIntervalInSeconds = 0 }));

        await watcher.SubscribeAllData(observer.Object, cts.Token);

        observer.Verify(o => o.OnDatasetVersionEnd(4), Times.Once);
        observer.Verify(o => o.OnCreate(Changed, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnCreate(Deleted, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnCreate(NewPage, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnCreate(NewRoot, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnChange(Changed, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnDelete(Deleted), Times.Once);
        observer.Verify(o => o.OnCreate(It.IsAny<string>(), It.IsAny<ReadOnlyMemory<byte>>()), Times.Exactly(4));
        observer.Verify(o => o.OnChange(It.IsAny<string>(), It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        observer.Verify(o => o.OnDelete(It.IsAny<string>()), Times.Once);
    }

    [Fact]
    public async Task DatasetUpdateReportingPlugin_ReportsTheFullNamesOfTreeDeltaEntries()
    {
        await _storageProvider.StartAsync(CancellationToken.None);
        await _writer.Put(Changed, [1], null, null);
        await WriteNewChangedAndDeleted();

        var reporter = new Mock<IDatasetUpdateReporting<object>>();
        reporter.SetupGet(r => r.ReportingIdentifier).Returns("test");
        reporter.Setup(r => r.OpenDeltaTransaction(It.IsAny<ulong>())).ReturnsAsync((object?)null);
        var plugin = new DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>(_storageProvider,
            Mock.Of<ILogger<DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>>>(), reporter.Object);

        await plugin.HandleNewDeltas(CancellationToken.None);

        var newNames = reporter.Invocations.Where(i => i.Method.Name == nameof(IDatasetUpdateReporting<object>.OnNewDto))
            .Select(i => (string)i.Arguments[0]);
        var changedNames = reporter.Invocations.Where(i => i.Method.Name == nameof(IDatasetUpdateReporting<object>.OnChangedDto))
            .Select(i => (string)i.Arguments[0]);
        var deletedNames = reporter.Invocations.Where(i => i.Method.Name == nameof(IDatasetUpdateReporting<object>.OnDeletedDto))
            .Select(i => (string)i.Arguments[0]);
        newNames.Should().BeEquivalentTo(Changed, Deleted, NewPage, NewRoot);
        changedNames.Should().Equal(Changed);
        deletedNames.Should().Equal(Deleted);
    }
}
