using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// The history returns, per change, the tracking id and SLA timestamp its write passed (VLOW-41).
/// </summary>
public class HistoryTrackingTests : IDisposable
{
    private const string Item = "t/T/items/I1/values";
    private static readonly DateTimeOffset Sla = new(2026, 10, 27, 12, 0, 0, TimeSpan.Zero);

    private readonly NotificationsHubSync<object> _hub = new();
    private readonly InMemoryStorageProvider<object> _storage;

    public HistoryTrackingTests()
    {
        _storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, _hub);
    }

    public void Dispose() => _hub.Dispose();

    /// <summary>
    /// Version 1 new with "A", the same bytes again with "B" (no version), version 2 changed with "C", version 3
    /// deleted with "D".
    /// </summary>
    private async Task<DatasetReaderWriter<object>> WriteTrackedChanges()
    {
        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await readerWriter.Put(Item, Encoding.UTF8.GetBytes("one"), "A", Sla);
        await readerWriter.Put(Item, Encoding.UTF8.GetBytes("one"), "B", null);
        await readerWriter.Put(Item, Encoding.UTF8.GetBytes("two"), "C", null);
        await readerWriter.Delete(Item, "D", Sla);
        return readerWriter;
    }

    private static readonly (string Id, ulong Version, ChangeType Change, string TrackingId, DateTimeOffset? Sla)[] Expected =
    [
        (Item, 3UL, ChangeType.Deleted, "D", Sla),
        (Item, 2UL, ChangeType.Changed, "C", null),
        (Item, 1UL, ChangeType.New, "A", Sla)
    ];

    private static IEnumerable<(string, ulong, ChangeType, string, DateTimeOffset?)> Tracked(IEnumerable<HistoryEntry> history) =>
        history.Select(h => (h.Id, h.Version, h.ChangeType, h.TrackingId, h.SlaTimestamp));

    [Fact]
    public async Task ReaderWriter_GetHistory_ReturnsTheTrackingIdOfTheWriteThatMadeEachVersion()
    {
        var readerWriter = await WriteTrackedChanges();

        var history = await readerWriter.GetHistory(Item);

        // The same bytes with "B" made no version, and version 1 keeps "A".
        Tracked(history).Should().Equal(Expected);
    }

    [Fact]
    public async Task Reader_GetHistory_ReturnsTheTrackingIdOfTheWriteThatMadeEachVersion()
    {
        await WriteTrackedChanges();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        var history = await reader.GetHistory(Item);

        Tracked(history).Should().Equal(Expected);
    }

    [Fact]
    public async Task Reader_StreamHistory_ReturnsTheTrackingIdOfEachChange()
    {
        await WriteTrackedChanges();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        var history = new List<HistoryEntry>();
        await foreach (var entry in reader.StreamHistory("t/T"))
            history.Add(entry);

        Tracked(history).Should().Equal(Expected);
    }

    [Fact]
    public async Task ReaderWriter_GetHistory_ReturnsAnEmptyTrackingId_WhenTheWritePassedNone()
    {
        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await readerWriter.Put(Item, Encoding.UTF8.GetBytes("one"), null, null);

        var entry = (await readerWriter.GetHistory(Item)).Should().ContainSingle().Subject;

        entry.TrackingId.Should().BeEmpty();
        entry.SlaTimestamp.Should().BeNull();
    }
}
