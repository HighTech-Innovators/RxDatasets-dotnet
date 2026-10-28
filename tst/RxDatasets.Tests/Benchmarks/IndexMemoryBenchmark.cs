using System.Collections.Immutable;
using System.Diagnostics;
using Google.Protobuf;
using RxDatasets.Models;
using Xunit.Abstractions;

namespace RxDatasets.Tests.Benchmarks;

/// <summary>Skipped unless the RXDATASETS_BENCHMARK environment variable is set; CI runs every other test.</summary>
public sealed class BenchmarkFactAttribute : FactAttribute
{
    public BenchmarkFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("RXDATASETS_BENCHMARK") is null)
            Skip = "Benchmark: set RXDATASETS_BENCHMARK=1 to run it";
    }
}

/// <summary>
/// The flat index (develop, before the tree) against the hierarchical index, with names shaped like the real DTOflow
/// ids. Run with <c>RXDATASETS_BENCHMARK=1 dotnet test -c Release --filter IndexMemoryBenchmark</c>, one test at a
/// time for stable memory numbers.
/// </summary>
public class IndexMemoryBenchmark(ITestOutputHelper output)
{
    private sealed record Dataset(int Count, Func<int, string> Name, string StoreParent, string TenantParent);

    /// <summary>storeitemvalues: <c>t/{tenantid}/s/{storeid}/items/{itemid}/values</c>, 100 stores × 10k items.</summary>
    private static Dataset StoreItemValues()
    {
        var random = new Random(42);
        var tenant = NewGuid(random);
        var stores = Enumerable.Range(0, 100).Select(_ => NewGuid(random)).ToArray();
        var items = Enumerable.Range(0, 10_000).Select(_ => random.Next().ToString("x8")).ToArray();
        return new Dataset(stores.Length * items.Length,
            i => $"t/{tenant}/s/{stores[i / items.Length]}/items/{items[i % items.Length]}/values",
            $"t/{tenant}/s/{stores[5]}", $"t/{tenant}");
    }

    /// <summary>
    /// studioeslimage: <c>t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/studioimage</c>, 100 stores × 1000
    /// ESLs × 3 pages.
    /// </summary>
    private static Dataset StudioEslImage()
    {
        var random = new Random(42);
        var tenant = NewGuid(random);
        var stores = Enumerable.Range(0, 100).Select(_ => NewGuid(random)).ToArray();
        const int eslsPerStore = 1000, pages = 3;
        var barcodes = Enumerable.Range(0, stores.Length * eslsPerStore)
            .Select(_ => random.NextInt64(10_000_000_000_000_000L, 99_999_999_999_999_999L).ToString()).ToArray();
        return new Dataset(barcodes.Length * pages,
            i => $"t/{tenant}/s/{stores[i / pages / eslsPerStore]}/esls/{barcodes[i / pages]}/pages/{i % pages + 1}/studioimage",
            $"t/{tenant}/s/{stores[5]}", $"t/{tenant}");
    }

    private static Guid NewGuid(Random random)
    {
        var bytes = new byte[16];
        random.NextBytes(bytes);
        return new Guid(bytes);
    }

    [BenchmarkFact]
    public void StoreItemValues_1M() => Run(StoreItemValues());

    [BenchmarkFact]
    public void StudioEslImage_300k() => Run(StudioEslImage());

    private void Run(Dataset dataset)
    {
        int count = dataset.Count;
        output.WriteLine($"{count:N0} entities, e.g. {dataset.Name(0)}");
        SnapshotIndex? flatProto = new SnapshotIndex { SnapshotVersion = 1 };
        for (int i = 0; i < count; i++)
        {
            flatProto.Entities.Add(dataset.Name(i),
                new SnapshotEntity { LastInsertedVersion = { 1 }, Position = i * 100L, Length = 100, Crc32 = (uint)i });
        }
        var flatBytes = flatProto.ToByteArray();
        flatProto = null;

        // Memory, excluding the entity records both indexes hold
        long before = Memory();
        ImmutableSnapshotEntity[]? records = SnapshotIndex.Parser.ParseFrom(flatBytes).Entities.Values
            .Select(entity => ImmutableSnapshotEntity.FromProto(entity)!).ToArray();
        double recordBytes = (Memory() - before) / (double)count;
        GC.KeepAlive(records);
        records = null;

        before = Memory();
        var flat = BuildFlat(SnapshotIndex.Parser.ParseFrom(flatBytes));
        double flatBytesPerEntity = (Memory() - before) / (double)count - recordBytes;

        before = Memory();
        var state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(flatBytes));
        double treeBytesPerEntity = (Memory() - before) / (double)count - recordBytes;
        Row("index memory per entity, excl. entity record",
            $"flat {flatBytesPerEntity:N0} B, tree {treeBytesPerEntity:N0} B (record {recordBytes:N0} B)");

        // Snapshot index.pb, and loading it
        var treeBytes = state.ToProto().ToByteArray();
        Row("snapshot index.pb", $"flat {flatBytes.Length / 1048576.0:N1} MB, tree {treeBytes.Length / 1048576.0:N1} MB");
        var watch = Stopwatch.StartNew();
        SnapshotIndex? parsedFlat = SnapshotIndex.Parser.ParseFrom(flatBytes);
        var parseFlat = watch.Elapsed;
        watch.Restart();
        GC.KeepAlive(BuildFlat(parsedFlat));
        Row("snapshot load, flat file, develop dictionary", $"{Ms(parseFlat)} parse + {Ms(watch.Elapsed)} build");
        watch.Restart();
        GC.KeepAlive(ImmutableDatasetState.FromProto(parsedFlat));
        Row("snapshot load, flat file, into the tree", $"{Ms(watch.Elapsed)} build");
        parsedFlat = null;
        Memory();
        watch.Restart();
        SnapshotIndex? parsedTree = SnapshotIndex.Parser.ParseFrom(treeBytes);
        var parseTree = watch.Elapsed;
        watch.Restart();
        GC.KeepAlive(ImmutableDatasetState.FromProto(parsedTree));
        Row("snapshot load, tree file", $"{Ms(parseTree)} parse + {Ms(watch.Elapsed)} build = {Ms(parseTree + watch.Elapsed)}");
        parsedTree = null;
        Memory();

        // Get
        var random = new Random(7);
        var lookups = Enumerable.Range(0, 200_000).Select(_ => dataset.Name(random.Next(count))).ToArray();
        foreach (var name in lookups.Take(1000))
            state.TryGetEntity(name, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread();
        watch.Restart();
        int hits = lookups.Count(name => state.TryGetEntity(name, out _));
        watch.Stop();
        allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
        Row("Get, tree",
            $"{watch.Elapsed.TotalMicroseconds / lookups.Length:N2} µs/op, {allocated / lookups.Length} B/op, {hits:N0} hits");
        watch.Restart();
        hits = lookups.Count(name => flat.ContainsKey(name));
        Row("Get, flat", $"{watch.Elapsed.TotalMicroseconds / lookups.Length:N2} µs/op, {hits:N0} hits");

        // ApplyDelta of 100k random entities
        var delta = new DeltaIndex { DeltaVersion = 2 };
        foreach (var i in Enumerable.Range(0, 100_000).Select(_ => random.Next(count)).Distinct())
        {
            delta.ChangedEntities.Add(
                new DeltaEntity { Name = dataset.Name(i), Position = i, Length = 100, Crc32 = 1, PreviousVersion = 1 });
        }
        int flatDeltaSize = delta.CalculateSize();
        watch.Restart();
        var flatAfter = flat;
        foreach (var change in delta.ChangedEntities)
        {
            flatAfter = flatAfter.SetItem(change.Name,
                flatAfter[change.Name] with { Version = 2, Position = change.Position });
        }
        Row($"ApplyDelta {delta.ChangedEntities.Count:N0}, flat (develop SetItem loop)", Ms(watch.Elapsed));
        GC.KeepAlive(flatAfter);
        delta.ConvertToTree();
        var treeDelta = DeltaIndex.Parser.ParseFrom(delta.ToByteArray());
        Memory();
        watch.Restart();
        GC.KeepAlive(state.ApplyDelta(treeDelta));
        Row($"ApplyDelta {treeDelta.EnumerateChangedEntities().Count():N0}, tree", Ms(watch.Elapsed));
        Row("delta index.pb", $"flat {flatDeltaSize / 1048576.0:N2} MB, tree {treeDelta.CalculateSize() / 1048576.0:N2} MB");

        // List and CountChildren, after a warm-up call
        state.Root.CountLive(dataset.TenantParent);
        watch.Restart();
        int listed = state.EnumerateAll(dataset.StoreParent).Count();
        Row($"List(store) -> {listed:N0}", Ms(watch.Elapsed));
        watch.Restart();
        int counted = state.Root.CountLive(dataset.StoreParent);
        Row($"CountChildren(store) -> {counted:N0}", $"{watch.Elapsed.TotalMicroseconds:N1} µs");
        watch.Restart();
        counted = state.Root.CountLive(dataset.TenantParent);
        Row($"CountChildren(tenant) -> {counted:N0}", $"{watch.Elapsed.TotalMicroseconds:N1} µs");

        GC.KeepAlive(flat);
        GC.KeepAlive(state);
    }

    private static ImmutableDictionary<string, ImmutableSnapshotEntity> BuildFlat(SnapshotIndex index)
    {
        var entities = new Dictionary<string, ImmutableSnapshotEntity>();
        foreach (var (name, entity) in index.Entities)
            entities.Add(name, ImmutableSnapshotEntity.FromProto(entity)!);
        return entities.ToImmutableDictionary();
    }

    private static long Memory()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        return GC.GetTotalMemory(true);
    }

    private static string Ms(TimeSpan elapsed) => $"{elapsed.TotalMilliseconds:N0} ms";

    private void Row(string what, string value) => output.WriteLine($"{what,-55} {value}");
}
