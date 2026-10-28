using System.Text;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// The names changed since a version, read from the deltas, each with the tracking id and SLA timestamp of its last
/// change (VLOW-41).
/// </summary>
public class HistoryChangesTests : IDisposable
{
    private const string Tracked = "t/T/items/I1/values";
    private const string Untracked = "t/T/items/I2/values";
    private const string Deleted = "t/T/items/I3/values";
    private const string Recreated = "t/T/items/I4/values";
    private const ulong Latest = 8;
    private static readonly DateTimeOffset Sla = new(2026, 10, 27, 12, 0, 0, TimeSpan.Zero);

    private readonly NotificationsHubSync<object> _hub = new();
    private readonly InMemoryStorageProvider<object> _storage;

    public HistoryChangesTests()
    {
        _storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, _hub);
    }

    public void Dispose() => _hub.Dispose();

    private async Task<DatasetReaderWriter<object>> ReaderWriter()
    {
        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        return readerWriter;
    }

    /// <summary>
    /// 1 <see cref="Tracked"/> new "A"; 2 <see cref="Untracked"/> new, no tracking id; 3 <see cref="Deleted"/> new "X";
    /// 4 <see cref="Tracked"/> changed "C" with an SLA; 5 <see cref="Deleted"/> deleted "D"; 6
    /// <see cref="Recreated"/> new "R1"; 7 deleted "R-"; 8 new again "R2".
    /// </summary>
    private async Task<DatasetReaderWriter<object>> WriteChanges()
    {
        var readerWriter = await ReaderWriter();
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("one"), "A", null);
        await readerWriter.Put(Untracked, Encoding.UTF8.GetBytes("two"), null, null);
        await readerWriter.Put(Deleted, Encoding.UTF8.GetBytes("three"), "X", null);
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("uno"), "C", Sla);
        await readerWriter.Delete(Deleted, "D", null);
        await readerWriter.Put(Recreated, Encoding.UTF8.GetBytes("four"), "R1", null);
        await readerWriter.Delete(Recreated, "R-", null);
        await readerWriter.Put(Recreated, Encoding.UTF8.GetBytes("vier"), "R2", null);
        return readerWriter;
    }

    [Fact]
    public async Task GetHistoryChanges_ReturnsTheNetChangeOfEachName_WithTheOriginOfItsLastChange()
    {
        var readerWriter = await WriteChanges();

        var changes = await readerWriter.GetHistoryChanges(3);

        // Created and deleted before 3 counts from its state at 3; deleted and created again since is a creation.
        changes.ToVersion.Should().Be(Latest);
        changes.Changes.Should().BeEquivalentTo(new[]
        {
            new HistoryChange(Tracked, ChangeType.Changed, "C", Sla),
            new HistoryChange(Deleted, ChangeType.Deleted, "D", null),
            new HistoryChange(Recreated, ChangeType.New, "R2", null)
        });
    }

    [Fact]
    public async Task GetHistoryChanges_ReportsWhatGetChangedNamesReports_FromEveryVersion()
    {
        var readerWriter = await WriteChanges();

        for (ulong from = 0; from <= Latest; from++)
        {
            var fromDeltas = await readerWriter.GetHistoryChanges(from);
            var fromIndex = await readerWriter.GetChangedNames(from);

            fromDeltas.Changes.Select(c => (c.Id, c.ChangeType)).Should().BeEquivalentTo(
                fromIndex.Changes.Select(c => (c.Name, c.ChangeType)), $"from version {from}");
        }
    }

    [Fact]
    public async Task Reader_GetHistoryChanges_ReadsTheSameDeltas()
    {
        await WriteChanges();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        var changes = await reader.GetHistoryChanges(0);

        changes.Changes.Should().BeEquivalentTo(new[]
        {
            new HistoryChange(Tracked, ChangeType.New, "C", Sla),
            new HistoryChange(Untracked, ChangeType.New, "", null),
            new HistoryChange(Recreated, ChangeType.New, "R2", null)
        });
    }

    [Fact]
    public async Task GetHistoryChanges_RequiresAResync_WhenADeltaInTheRangeIsNoLongerKept()
    {
        var readerWriter = await WriteChanges();
        await _storage.DeleteOlderThan(2);

        var sinceCleanup = await readerWriter.GetHistoryChanges(2);
        var beforeCleanup = () => readerWriter.GetHistoryChanges(1);

        sinceCleanup.Changes.Select(c => c.Id).Should().BeEquivalentTo(Tracked, Recreated);
        (await beforeCleanup.Should().ThrowExactlyAsync<ResyncRequiredException>()).Which.FromVersion.Should().Be(1);
    }

    [Fact]
    public async Task GetHistoryChanges_IsEmptyAtTheLatestVersion_AndRefusesANewerOne()
    {
        var readerWriter = await WriteChanges();

        (await readerWriter.GetHistoryChanges(Latest)).Changes.Should().BeEmpty();
        await ((Func<Task>)(() => readerWriter.GetHistoryChanges(Latest + 1))).Should().ThrowAsync<ArgumentOutOfRangeException>();
    }

    [Fact]
    public async Task WaitHistoryChanges_AnswersOnceAWriteLands()
    {
        var readerWriter = await ReaderWriter();
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("one"), "A", null);

        var waiting = readerWriter.WaitHistoryChanges(1);
        await Task.Delay(50);
        waiting.IsCompleted.Should().BeFalse("nothing changed since version 1");
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("uno"), "B", null);

        (await waiting.WaitAsync(TimeSpan.FromSeconds(5))).Changes.Should().Equal(new HistoryChange(Tracked, ChangeType.Changed, "B", null));
    }

    [Fact]
    public async Task ListHistoryChanges_KeepsTheOriginsOnAPageServedFromTheCache()
    {
        var dataset = Substitute.For<IDatasetReader<Demodto>>();
        dataset.GetHistoryChanges(0, Arg.Any<CancellationToken>()).Returns(new HistoryChangeSet(5,
        [
            new HistoryChange(Tracked, ChangeType.Changed, "C", Sla),
            new HistoryChange(Untracked, ChangeType.New, "", null)
        ]));
        var sut = GrpcReader(dataset);

        var first = await sut.ListHistoryChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, "", false), default);
        var second = await sut.ListHistoryChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, first.NextPageToken, false), default);

        first.Changes.Should().Equal(new HistoryChange(Tracked, ChangeType.Changed, "C", Sla));
        second.Changes.Should().Equal(new HistoryChange(Untracked, ChangeType.New, "", null));
        second.NextPageToken.Should().BeEmpty();
        second.ToDatasetVersion.Should().Be(5);
    }

    [Fact]
    public async Task ListHistoryChanges_AnswersOutOfRange_WhenAResyncIsRequired()
    {
        var dataset = Substitute.For<IDatasetReader<Demodto>>();
        dataset.GetHistoryChanges(1, Arg.Any<CancellationToken>()).Returns<HistoryChangeSet>(_ => throw new ResyncRequiredException(1));
        var sut = GrpcReader(dataset);

        var listing = () => sut.ListHistoryChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(1, 10, "", false), default);

        (await listing.Should().ThrowExactlyAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.OutOfRange);
    }

    [Fact]
    public async Task ListHistoryChanges_DoesNotContinueAListChangesPageToken()
    {
        var dataset = Substitute.For<IDatasetReader<Demodto>>();
        dataset.GetChangedNames(0).Returns(new ChangeSet(5, [(Tracked, ChangeType.Changed), (Untracked, ChangeType.New)]));
        var sut = GrpcReader(dataset);
        var changes = await sut.ListChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, "", false), default);

        var continuation = () => sut.ListHistoryChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, changes.NextPageToken, false), default);

        (await continuation.Should().ThrowExactlyAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    private static GrpcReaderServiceImpl<Demodto> GrpcReader(IDatasetReader<Demodto> dataset)
    {
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        configurationSection.Value.Returns("60");
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        return new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(), configuration, dataset, new ServiceCollection().BuildServiceProvider());
    }
}
