using System.Diagnostics;
using K4os.Compression.LZ4;
using RxDatasets.Models;

const string filePath = @"I:\Temp\index.pb.lz4";
const string pattern = "t/{tenantid}/s/{storeid}/items/{itemid}/values";
const string outputPath = @"I:\Temp\index-compressed.pb";

Console.WriteLine("=== Streaming Index Codec Benchmark ===");
Console.WriteLine($"File: {filePath}");
Console.WriteLine($"Compressed: {new FileInfo(filePath).Length / 1024.0 / 1024.0:F1} MB");
Console.WriteLine();

GC.Collect(2, GCCollectionMode.Aggressive, true, true);
GC.WaitForPendingFinalizers();
long baseMemory = GC.GetTotalMemory(true);
Console.WriteLine($"Baseline memory: {baseMemory / 1024.0 / 1024.0:F1} MB");
Console.WriteLine();

// ── Step 1: Read + decompress ──
var sw = Stopwatch.StartNew();
var compressed = File.ReadAllBytes(filePath);
sw.Stop();
Console.WriteLine($"[1] File read:        {sw.ElapsedMilliseconds,6} ms  | compressed: {compressed.Length / 1024.0 / 1024.0:F1} MB");

sw.Restart();
var decompressed = LZ4Pickler.Unpickle(compressed);
sw.Stop();
compressed = null!;
GC.Collect(0);
Console.WriteLine($"[2] LZ4 decompress:   {sw.ElapsedMilliseconds,6} ms  | decompressed: {decompressed.Length / 1024.0 / 1024.0:F1} MB");

// ── Step 3a: Streaming read, NO compressor (old-style full trie) ──
Console.WriteLine();
Console.WriteLine("══ 3a: Read old file → full trie (no compressor) ══");
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
long before3a = GC.GetTotalMemory(true);

sw.Restart();
var result3a = StreamingIndexCodec.ReadSnapshot(decompressed, null);
sw.Stop();
long after3a = GC.GetTotalMemory(false);

Console.WriteLine($"    Parse:   {sw.ElapsedMilliseconds,6} ms  | memory: {(after3a - before3a) / 1024.0 / 1024.0:F1} MB");
Console.WriteLine($"    Version: {result3a.Version}  |  Entities: {result3a.State.Entities.Count:N0}");

var entityCount = result3a.State.Entities.Count;
var version = result3a.Version;

foreach (var (name, e) in result3a.State.Entities.Take(3))
    Console.WriteLine($"    {name}  pos={e.Position} len={e.Length} v={e.LatestVersion}");

result3a = default;
GC.Collect(2, GCCollectionMode.Aggressive, true, true);

// ── Step 3b: Streaming read, WITH compressor (pattern transition: uncompressed → compressed) ──
Console.WriteLine();
Console.WriteLine("══ 3b: Read old file → compressed trie (pattern transition) ══");
Console.WriteLine($"    Pattern: {pattern}");
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
long before3b = GC.GetTotalMemory(true);

sw.Restart();
var result3b = StreamingIndexCodec.ReadSnapshot(decompressed, NameCompressor.FromPattern(pattern));
sw.Stop();
long after3b = GC.GetTotalMemory(false);

Console.WriteLine($"    Parse:   {sw.ElapsedMilliseconds,6} ms  | memory: {(after3b - before3b) / 1024.0 / 1024.0:F1} MB");
Console.WriteLine($"    Version: {result3b.Version}  |  Entities: {result3b.State.Entities.Count:N0}");
Console.WriteLine($"    Match: {result3b.State.Entities.Count == entityCount}");

foreach (var (name, e) in result3b.State.Entities.Take(3))
    Console.WriteLine($"    {name}  pos={e.Position} len={e.Length} v={e.LatestVersion}");

result3b = default;
GC.Collect(2, GCCollectionMode.Aggressive, true, true);

// ── Step 4: Write in new ID-compressed format ──
// Re-read without compressor to get entity data, then compress + write
Console.WriteLine();
Console.WriteLine("══ 4: Write in compressed format ══");
Console.WriteLine($"    Pattern: {pattern}");

var compressor = NameCompressor.FromPattern(pattern)!;
var sourceResult = StreamingIndexCodec.ReadSnapshot(decompressed, null);

sw.Restart();
var protoEntities = new Dictionary<string, SnapshotEntity>(entityCount);
foreach (var (fullName, ie) in sourceResult.State.Entities)
{
    var compressedName = compressor.Compress(fullName);
    var se = new SnapshotEntity { Position = ie.Position, Length = ie.Length, Crc32 = ie.Crc32 };
    se.LastInsertedVersion.Add(ie.LatestVersion);
    protoEntities[compressedName] = se;
}
sw.Stop();
Console.WriteLine($"    Compress names: {sw.ElapsedMilliseconds,6} ms");

sourceResult = default;
GC.Collect(0);

sw.Restart();
var bucket = BuildNestedSnapshotBucket(protoEntities);
sw.Stop();
Console.WriteLine($"    Build bucket:   {sw.ElapsedMilliseconds,6} ms");

protoEntities = null!;
GC.Collect(0);

sw.Restart();
var newBytes = StreamingIndexCodec.WriteSnapshot(version, compressor.Pattern, bucket);
sw.Stop();
Console.WriteLine($"    Serialize:      {sw.ElapsedMilliseconds,6} ms  | output: {newBytes.Length / 1024.0 / 1024.0:F1} MB");
Console.WriteLine($"    Size: {(double)newBytes.Length / decompressed.Length:P1} of original {decompressed.Length / 1024.0 / 1024.0:F1} MB");

File.WriteAllBytes(outputPath, newBytes);
Console.WriteLine($"    Written to: {outputPath}");

bucket = null!;
newBytes = null!;
GC.Collect(2, GCCollectionMode.Aggressive, true, true);

// ── Step 5: Read back the compressed file (matching pattern — fast path) ──
Console.WriteLine();
Console.WriteLine("══ 5: Read compressed file → compressed trie (pattern match — fast path) ══");
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
long before5 = GC.GetTotalMemory(true);

var rereadBytes = File.ReadAllBytes(outputPath);
sw.Restart();
var result5 = StreamingIndexCodec.ReadSnapshot(rereadBytes, compressor);
sw.Stop();
long after5 = GC.GetTotalMemory(false);

Console.WriteLine($"    Parse:   {sw.ElapsedMilliseconds,6} ms  | memory: {(after5 - before5) / 1024.0 / 1024.0:F1} MB");
Console.WriteLine($"    Version: {result5.Version}  |  Entities: {result5.State.Entities.Count:N0}");
Console.WriteLine($"    Match: {result5.State.Entities.Count == entityCount}  |  Version: {result5.Version == version}");

foreach (var (name, e) in result5.State.Entities.Take(3))
    Console.WriteLine($"    {name}  pos={e.Position} len={e.Length} v={e.LatestVersion}");

// ── Summary ──
Console.WriteLine();
Console.WriteLine("══ SUMMARY ══");
Console.WriteLine($"  3a) Old file, no compressor:    → full 7-level trie");
Console.WriteLine($"  3b) Old file + compressor:      → transition: stream + rebuild compressed 3-level trie");
Console.WriteLine($"  5)  Compressed file + compressor: → direct match: stream into compressed 3-level trie");
Console.WriteLine($"  Disk savings: {decompressed.Length / 1024.0 / 1024.0:F1} MB → {new FileInfo(outputPath).Length / 1024.0 / 1024.0:F1} MB");

// Clean up
decompressed = null!;
rereadBytes = null!;
result5 = default;
GC.Collect(2, GCCollectionMode.Aggressive, true, true);
GC.WaitForPendingFinalizers();
long finalMemory = GC.GetTotalMemory(true);
Console.WriteLine($"  Final memory: {finalMemory / 1024.0 / 1024.0:F1} MB  (baseline: {baseMemory / 1024.0 / 1024.0:F1} MB)");
Console.WriteLine("Done.");

// ── Helper ──
static SnapshotBucket BuildNestedSnapshotBucket(Dictionary<string, SnapshotEntity> entities)
{
    var root = new SnapshotBucket();
    foreach (var (name, entity) in entities)
    {
        var segments = name.Split('/');
        var current = root;
        for (int i = 0; i < segments.Length - 1; i++)
        {
            if (!current.Children.TryGetValue(segments[i], out var child))
            {
                child = new SnapshotBucket();
                current.Children[segments[i]] = child;
            }
            current = child;
        }
        current.Leaves[segments[^1]] = entity;
    }
    return root;
}
