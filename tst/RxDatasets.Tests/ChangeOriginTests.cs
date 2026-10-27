using System.Text;
using Google.Protobuf;
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
using RxDatasets.Models;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// A change list returns, per changed name, the tracking id and SLA timestamp of the write that made its latest
/// change (VLOW-41).
/// </summary>
public class ChangeOriginTests : IDisposable
{
    private const string Tracked = "t/T/items/I1/values";
    private const string Untracked = "t/T/items/I2/values";
    private const string Deleted = "t/T/items/I3/values";
    private static readonly DateTimeOffset Sla = new(2026, 10, 27, 12, 0, 0, TimeSpan.Zero);

    private readonly NotificationsHubSync<object> _hub = new();
    private readonly InMemoryStorageProvider<object> _storage;

    public ChangeOriginTests()
    {
        _storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, _hub);
    }

    public void Dispose() => _hub.Dispose();

    /// <summary>
    /// Version 1 <see cref="Tracked"/> new with "A", 2 <see cref="Untracked"/> new without a tracking id, 3
    /// <see cref="Deleted"/> new with "X", 4 <see cref="Tracked"/> changed with "C" and an SLA, 5
    /// <see cref="Deleted"/> deleted with "D".
    /// </summary>
    private async Task<DatasetReaderWriter<object>> WriteChanges()
    {
        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("one"), "A", null);
        await readerWriter.Put(Untracked, Encoding.UTF8.GetBytes("two"), null, null);
        await readerWriter.Put(Deleted, Encoding.UTF8.GetBytes("three"), "X", null);
        await readerWriter.Put(Tracked, Encoding.UTF8.GetBytes("uno"), "C", Sla);
        await readerWriter.Delete(Deleted, "D", null);
        return readerWriter;
    }

    [Fact]
    public async Task ReaderWriter_GetChangedNames_ReturnsTheOriginOfEachNamesLatestChange()
    {
        var readerWriter = await WriteChanges();

        var changes = await readerWriter.GetChangedNames(3);

        // The origin of the latest change: "C" for the change, not "A" for the creation; "D" for the delete.
        changes.Origins.Should().BeEquivalentTo(new Dictionary<string, ChangeOrigin>
        {
            [Tracked] = new("C", Sla),
            [Deleted] = new("D", null)
        });
    }

    [Fact]
    public async Task Reader_GetChangedNames_ReturnsTheOriginOfEachNamesLatestChange()
    {
        await WriteChanges();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        var changes = await reader.GetChangedNames(0);

        // A name whose write passed neither has no origin; one created and deleted since 0 is not a change.
        changes.Changes.Select(c => c.Name).Should().BeEquivalentTo(Tracked, Untracked);
        changes.Origins.Should().BeEquivalentTo(new Dictionary<string, ChangeOrigin> { [Tracked] = new("C", Sla) });
    }

    [Fact]
    public void Snapshot_KeepsTheOriginOfEachEntity()
    {
        var state = ImmutableDatasetState.FromEntities(
        [
            new(Tracked, new ImmutableSnapshotEntity([], [1], 4, 0, 3) { Origin = new ChangeOrigin("C", Sla) }),
            new(Untracked, new ImmutableSnapshotEntity([], [2], 0, 3, 3))
        ], 4, 4);

        var restored = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(state.ToProto().ToByteArray()));

        restored.GetEntity(Tracked).Origin.Should().Be(new ChangeOrigin("C", Sla));
        restored.GetEntity(Untracked).Origin.Should().BeNull();
    }

    [Fact]
    public async Task ListChanges_ReturnsTheOriginsOfTheNamesOnEachPage()
    {
        var dataset = Substitute.For<IDatasetReader<Demodto>>();
        dataset.GetChangedNames(0).Returns(new ChangeSet(5, [(Tracked, ChangeType.Changed), (Untracked, ChangeType.New)])
        {
            Origins = new Dictionary<string, ChangeOrigin> { [Tracked] = new("C", Sla) }
        });
        var sut = GrpcReader(dataset);

        var first = await sut.ListChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, "", false), default);
        var second = await sut.ListChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(0, 1, first.NextPageToken, false), default);

        // The second page comes from the cache, and keeps its origins too.
        first.ChangedEntities.Should().Equal(Tracked);
        first.Origins.Should().BeEquivalentTo(new Dictionary<string, ChangeOrigin> { [Tracked] = new("C", Sla) });
        second.NewEntities.Should().Equal(Untracked);
        second.Origins.Should().BeEmpty();
    }

    [Fact]
    public void ListChangesResponse_HasNoOrigins_WhenDefault()
    {
        default(GrpcReaderServiceImpl<Demodto>.ListChangesResponse).Origins.Should().BeEmpty();
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
