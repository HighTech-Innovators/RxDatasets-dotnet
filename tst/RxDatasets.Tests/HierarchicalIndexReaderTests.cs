using System.Text;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// A dataset whose first deltas are in the legacy flat format, continued by a writer that writes the tree format.
/// </summary>
public class HierarchicalIndexReaderTests : IDisposable
{
    private const string Item1 = "t/T/s/S/items/I1/values";
    private const string Item2 = "t/T/s/S/items/I2/values";
    private const string Tenant = "t/T/tenant";

    private readonly NotificationsHubSync<object> _hub = new();
    private readonly InMemoryStorageProvider<object> _storage;

    public HierarchicalIndexReaderTests()
    {
        _storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, _hub);
    }

    public void Dispose() => _hub.Dispose();

    /// <summary>A legacy delta: flat lists with full names, and its data file.</summary>
    private async Task StoreLegacyDelta(ulong version, (string Name, string Value, ulong PreviousVersion)[] news,
        (string Name, string Value, ulong PreviousVersion)[] changes, string[] deletedNames)
    {
        var index = new DeltaIndex { DeltaVersion = version, Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow) };
        var data = new MemoryStream();
        DeltaEntity Write(string name, string value, ulong previousVersion)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            data.WriteByte((byte)bytes.Length);
            var entity = new DeltaEntity
            {
                Name = name, Position = data.Position, Length = bytes.Length, PreviousVersion = previousVersion
            };
            data.Write(bytes);
            return entity;
        }

        index.NewEntities.AddRange(news.Select(n => Write(n.Name, n.Value, n.PreviousVersion)));
        index.ChangedEntities.AddRange(changes.Select(c => Write(c.Name, c.Value, c.PreviousVersion)));
        index.DeletedNames.AddRange(deletedNames);
        data.Position = 0;
        await _storage.StoreDelta(version, index.ToByteArray(), data);
    }

    private async Task<DatasetReaderWriter<object>> ContinueWithTreeDeltas()
    {
        await StoreLegacyDelta(1, [(Item1, "one", 0), (Tenant, "tenant", 0)], [], []);
        await StoreLegacyDelta(2, [], [(Item1, "uno", 1)], [Tenant]);

        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await readerWriter.Put(Item2, Encoding.UTF8.GetBytes("two"), null, null);
        await readerWriter.Delete(Item1, null, null);
        return readerWriter;
    }

    private static string? Text((string name, ReadOnlyMemory<byte> entityBytes)? entity) =>
        entity is { } e ? Encoding.UTF8.GetString(e.entityBytes.Span) : null;

    [Fact]
    public async Task Writer_WritesTreeDeltas_WithEmptyNames()
    {
        await ContinueWithTreeDeltas();

        var delta = DeltaIndex.Parser.ParseFrom((await _storage.ReadDeltaIndex(3)).Span);

        delta.HasFlatEntries().Should().BeFalse();
        delta.EnumerateNewEntities().Should().ContainSingle()
            .Which.Should().Match<(string Name, DeltaEntity Entity)>(e => e.Name == Item2 && e.Entity.Name == "");
    }

    [Fact]
    public async Task ReaderWriter_ReadsAcrossLegacyAndTreeDeltas()
    {
        var readerWriter = await ContinueWithTreeDeltas();

        Text(await readerWriter.Get(Item2)).Should().Be("two");
        (await readerWriter.Get(Item1)).Should().BeNull();
        (await readerWriter.Get(Tenant)).Should().BeNull();
        (await readerWriter.List("t/T/s/S/items")).Names.Should().Equal(Item2);
        (await readerWriter.CountChildren("t/T")).Should().Be(1);
        (await readerWriter.GetHistory(Item1)).Select(h => (h.Version, h.ChangeType, h.PreviousVersion)).Should().Equal(
            (4UL, ChangeType.Deleted, 2UL), (2UL, ChangeType.Changed, 1UL), (1UL, ChangeType.New, 0UL));
        Text(await readerWriter.GetHistoric(Item1, 2)).Should().Be("uno");
        (await readerWriter.GetHistoric(Item1, 4)).Should().BeNull();
    }

    [Fact]
    public async Task Reader_ReadsAcrossLegacyAndTreeDeltas()
    {
        await ContinueWithTreeDeltas();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        Text(await reader.Get(Item2)).Should().Be("two");
        (await reader.List("")).Names.Should().Equal(Item2);
        (await reader.CountChildren("t/T/s/S/items")).Should().Be(1);
        var history = new List<HistoryEntry>();
        await foreach (var entry in reader.StreamHistory("t/T/s/S"))
            history.Add(entry);
        history.Select(h => (h.Id, h.Version, h.ChangeType)).Should().Equal(
            (Item1, 4UL, ChangeType.Deleted), (Item2, 3UL, ChangeType.New), (Item1, 2UL, ChangeType.Changed),
            (Item1, 1UL, ChangeType.New));
        var tail = new List<HistoryEntry>();
        await foreach (var entry in reader.TailHistory(""))
            tail.Add(entry);
        tail.Select(h => (h.Id, h.Version)).Should().Equal(
            (Item1, 1UL), (Tenant, 1UL), (Item1, 2UL), (Tenant, 2UL), (Item2, 3UL), (Item1, 4UL));
        await reader.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ListAndCountChildren_TreatNullAsEverything()
    {
        var readerWriter = await ContinueWithTreeDeltas();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        (await readerWriter.List(null!)).Names.Should().Equal(Item2);
        (await readerWriter.CountChildren(null!)).Should().Be(1);
        (await reader.List(null!)).Names.Should().Equal(Item2);
        (await reader.CountChildren(null!)).Should().Be(1);
        await reader.StopAsync(CancellationToken.None);
    }
}
