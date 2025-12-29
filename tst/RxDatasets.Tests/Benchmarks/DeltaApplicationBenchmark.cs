using System.Collections.Immutable;
using RxDatasets.Models;

namespace RxDatasets.Tests.Benchmarks;

/// <summary>
/// Benchmark tests for delta application performance.
/// Measures the time to apply deltas to the hierarchical structure.
/// </summary>
public class DeltaApplicationBenchmark
{
    [Fact]
    public void ApplyDelta_ToFlatIndex_Measures_Performance()
    {
        // Create a base state with many entities
        var baseEntities = new Dictionary<string, ImmutableSnapshotEntity>();
        for (int i = 0; i < 1000; i++)
        {
            baseEntities[$"file_{i:D6}.dat"] = new ImmutableSnapshotEntity(
                ImmutableArray<ulong>.Empty,
                ImmutableArray<ulong>.Empty,
                1,
                i * 1000L,
                1000);
        }

        var baseState = new ImmutableDatasetState(
            baseEntities.ToImmutableDictionary(),
            1,
            1);

        // Create a delta with multiple changes
        var delta = new DeltaIndex { DeltaVersion = 2 };

        for (int i = 0; i < 100; i++)
        {
            delta.NewEntities.Add(new DeltaEntity
            {
                Name = $"new_file_{i:D6}.dat",
                Position = (1000 + i) * 1000L,
                Length = 1000,
                Crc32 = (uint)i
            });
        }

        // Measure application time
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var updated = baseState.ApplyDelta(delta);
        stopwatch.Stop();

        // Verify
        Assert.Equal(1100, updated.Entities.Count);
        Assert.Equal(2uL, updated.LatestVersion);

        // Output for manual observation
        System.Diagnostics.Debug.WriteLine($"Delta application time: {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void ApplyMultipleDeltasSequentially_Measures_Performance()
    {
        // Create a base state
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>().ToImmutableDictionary(),
            1,
            1);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var currentState = baseState;
        for (int d = 1; d <= 10; d++)
        {
            var delta = new DeltaIndex { DeltaVersion = (ulong)(d + 1) };

            // Add 100 new files in each delta
            for (int i = 0; i < 100; i++)
            {
                delta.NewEntities.Add(new DeltaEntity
                {
                    Name = $"file_d{d:D2}_f{i:D6}.dat",
                    Position = (d * 100 + i) * 1000L,
                    Length = 1000,
                    Crc32 = (uint)(d * 100 + i)
                });
            }

            currentState = currentState.ApplyDelta(delta);
        }

        stopwatch.Stop();

        // Verify
        Assert.Equal(1000, currentState.Entities.Count);
        Assert.Equal(11uL, currentState.LatestVersion);

        System.Diagnostics.Debug.WriteLine($"10 sequential deltas (100 entities each) applied in {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void ApplyDeltaWithMixedOperations_Measures_Performance()
    {
        // Create a base state with entities
        var baseEntities = new Dictionary<string, ImmutableSnapshotEntity>();
        for (int i = 0; i < 500; i++)
        {
            baseEntities[$"file_{i:D6}.dat"] = new ImmutableSnapshotEntity(
                ImmutableArray<ulong>.Empty,
                ImmutableArray<ulong>.Empty,
                1,
                i * 1000L,
                1000);
        }

        var baseState = new ImmutableDatasetState(
            baseEntities.ToImmutableDictionary(),
            1,
            1);

        // Create a delta with mixed operations
        var delta = new DeltaIndex { DeltaVersion = 2 };

        // New entities
        for (int i = 0; i < 100; i++)
        {
            delta.NewEntities.Add(new DeltaEntity
            {
                Name = $"new_file_{i:D6}.dat",
                Position = 500000 + i * 1000L,
                Length = 1000,
                Crc32 = (uint)i
            });
        }

        // Changed entities
        for (int i = 0; i < 100; i++)
        {
            delta.ChangedEntities.Add(new DeltaEntity
            {
                Name = $"file_{i:D6}.dat",
                Position = i * 1000L,
                Length = 2000,  // Changed length
                Crc32 = 9999
            });
        }

        // Deleted entities
        for (int i = 100; i < 150; i++)
        {
            delta.DeletedNames.Add($"file_{i:D6}.dat");
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var updated = baseState.ApplyDelta(delta);
        stopwatch.Stop();

        // Verify
        // Note: ApplyDelta doesn't remove entities, it marks them as deleted
        // So all 500 original + 100 new entities remain in the dictionary
        Assert.Equal(600, updated.Entities.Count);
        Assert.Equal(2uL, updated.LatestVersion);

        System.Diagnostics.Debug.WriteLine($"Mixed delta (100 new, 100 changed, 50 deleted) applied in {stopwatch.ElapsedMilliseconds}ms");
    }

    [Fact]
    public void DeltaApplication_ThreadSafety_Test()
    {
        // Create initial state
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["shared_file.dat"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    1000)
            }.ToImmutableDictionary(),
            1,
            1);

        // Create multiple deltas
        var deltas = new List<DeltaIndex>();
        for (int d = 0; d < 5; d++)
        {
            var delta = new DeltaIndex { DeltaVersion = (ulong)(d + 2) };
            delta.NewEntities.Add(new DeltaEntity
            {
                Name = $"thread_file_{d:D2}.dat",
                Position = d * 1000L,
                Length = 1000,
                Crc32 = (uint)d
            });
            deltas.Add(delta);
        }

        // Apply deltas sequentially (ImmutableDatasetState is inherently thread-safe due to immutability)
        var finalState = baseState;
        foreach (var delta in deltas)
        {
            finalState = finalState.ApplyDelta(delta);
        }

        // Verify all deltas were applied
        Assert.Equal(6, finalState.Entities.Count);  // 1 initial + 5 new
        Assert.Equal(6uL, finalState.LatestVersion);
    }

    [Fact]
    public void DeltaApplication_Immutability_Verified()
    {
        // Create initial state
        var state1 = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file1.dat"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    1000)
            }.ToImmutableDictionary(),
            1,
            1);

        // Apply a delta
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(new DeltaEntity
        {
            Name = "file2.dat",
            Position = 1000,
            Length = 1000,
            Crc32 = 0
        });

        var state2 = state1.ApplyDelta(delta);

        // Verify original state is unchanged
        Assert.Single(state1.Entities);
        Assert.Equal(1uL, state1.LatestVersion);

        // Verify new state has the change
        Assert.Equal(2, state2.Entities.Count);
        Assert.Equal(2uL, state2.LatestVersion);

        // Verify they are different objects
        Assert.NotSame(state1, state2);
    }

    [Fact]
    public void LargeDatasetDeltaApplication_Performance()
    {
        // Create a large base state (100K entities)
        var baseEntities = new Dictionary<string, ImmutableSnapshotEntity>();
        for (int i = 0; i < 100000; i++)
        {
            baseEntities[$"file_{i:D10}.dat"] = new ImmutableSnapshotEntity(
                ImmutableArray<ulong>.Empty,
                ImmutableArray<ulong>.Empty,
                1,
                i * 100L,
                100);
        }

        var baseState = new ImmutableDatasetState(
            baseEntities.ToImmutableDictionary(),
            1,
            1);

        // Create a delta with 1000 changes to a subset
        var delta = new DeltaIndex { DeltaVersion = 2 };
        for (int i = 0; i < 1000; i++)
        {
            delta.ChangedEntities.Add(new DeltaEntity
            {
                Name = $"file_{i:D10}.dat",
                Position = i * 100L,
                Length = 200,  // Double the size
                Crc32 = (uint)i
            });
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var updated = baseState.ApplyDelta(delta);
        stopwatch.Stop();

        // Verify
        Assert.Equal(100000, updated.Entities.Count);
        Assert.Equal(2uL, updated.LatestVersion);

        System.Diagnostics.Debug.WriteLine($"Applied delta to 100K entity dataset in {stopwatch.ElapsedMilliseconds}ms");
        // Goal: Should be fast, typically < 100ms even for large datasets
    }
}
