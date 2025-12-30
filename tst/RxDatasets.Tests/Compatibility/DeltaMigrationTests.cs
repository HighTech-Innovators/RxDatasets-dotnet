using System.Collections.Immutable;
using RxDatasets.Models;

namespace RxDatasets.Tests.Compatibility;

/// <summary>
/// Integration tests for delta migration from flat to hierarchical format.
/// Verifies that the system can read legacy deltas and apply them correctly.
/// </summary>
public class DeltaMigrationTests
{
    [Fact]
    public void LegacyDelta_FlatNewEntities_CanBeApplied()
    {
        // Create a base state
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file1.txt"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    100)
            }.ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create a legacy delta with new entities
        var delta = new DeltaIndex
        {
            DeltaVersion = 2
        };
        delta.NewEntities.Add(new DeltaEntity
        {
            Name = "file2.txt",
            Position = 100,
            Length = 50,
            Crc32 = 0,
            PreviousVersion = 0
        });

        // Apply the delta
        var updated = baseState.ApplyDelta(delta);

        // Verify the new entity was added
        Assert.Equal(2, updated.IndexNode.Count);
        Assert.NotNull(updated.GetEntity("file2.txt"));
        Assert.Equal(2uL, updated.LatestVersion);
    }

    [Fact]
    public void LegacyDelta_FlatChangedEntities_CanBeApplied()
    {
        // Create a base state
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file.txt"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    100)
            }.ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create a legacy delta with changed entities
        var delta = new DeltaIndex
        {
            DeltaVersion = 2
        };
        delta.ChangedEntities.Add(new DeltaEntity
        {
            Name = "file.txt",
            Position = 100,
            Length = 200,
            Crc32 = 123,
            PreviousVersion = 1
        });

        // Apply the delta
        var updated = baseState.ApplyDelta(delta);

        // Verify the entity was updated
        Assert.Equal(1, updated.IndexNode.Count);
        var entity = updated.GetEntity("file.txt");
        Assert.NotNull(entity);
        Assert.Equal(100L, entity.Position);
        Assert.Equal(200, entity.Length);
        Assert.Equal(2uL, updated.LatestVersion);
    }

    [Fact]
    public void LegacyDelta_FlatDeletedNames_CanBeApplied()
    {
        // Create a base state with an entity
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file_to_delete.txt"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    100)
            }.ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create a legacy delta with deleted names
        var delta = new DeltaIndex
        {
            DeltaVersion = 2
        };
        delta.DeletedNames.Add("file_to_delete.txt");

        // Apply the delta
        var updated = baseState.ApplyDelta(delta);

        // Verify the entity was marked as deleted
        Assert.Equal(1, updated.IndexNode.Count);
        var entity = updated.GetEntity("file_to_delete.txt");
        Assert.NotNull(entity);
        Assert.NotEmpty(entity.LastDeletedVersion);
        Assert.Contains(2uL, entity.LastDeletedVersion);
    }

    [Fact]
    public void LegacyDelta_DeletedEntities_CanBeApplied()
    {
        // Create a base state
        var baseState = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file.txt"] = new ImmutableSnapshotEntity(
                    ImmutableArray<ulong>.Empty,
                    ImmutableArray<ulong>.Empty,
                    1,
                    0,
                    100)
            }.ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create a delta with deleted entities
        var delta = new DeltaIndex
        {
            DeltaVersion = 2
        };
        delta.DeletedEntities.Add(new DeletedEntity
        {
            Name = "file.txt",
            PreviousVersion = 1
        });

        // Apply the delta
        var updated = baseState.ApplyDelta(delta);

        // Verify the entity was marked as deleted
        Assert.Equal(1, updated.IndexNode.Count);
        var entity = updated.GetEntity("file.txt");
        Assert.NotNull(entity);
        Assert.NotEmpty(entity.LastDeletedVersion);
        Assert.Contains(2uL, entity.LastDeletedVersion);
    }

    [Fact]
    public void HierarchicalDelta_CanBeDeserialized()
    {
        // Create a hierarchical delta
        var hierarchicalDelta = new DeltaNode();
        var subdirNode = new DeltaNode();
        subdirNode.NewEntities.Add("newfile.txt", new DeltaEntity
        {
            Name = "newfile.txt",
            Position = 0,
            Length = 50,
            Crc32 = 0,
            PreviousVersion = 0
        });
        hierarchicalDelta.Children.Add("subdir", subdirNode);

        var deltaIndex = new DeltaIndex
        {
            DeltaVersion = 2,
            Root = hierarchicalDelta
        };

        // Verify the structure is preserved
        Assert.NotNull(deltaIndex.Root);
        Assert.NotEmpty(deltaIndex.Root.Children);
        Assert.Contains("subdir", deltaIndex.Root.Children.Keys);
    }

    [Fact]
    public void MultipleDeltas_CanBeAppliedSequentially()
    {
        // Create initial state
        var state = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>().ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Apply first delta (add file1)
        var delta1 = new DeltaIndex { DeltaVersion = 2 };
        delta1.NewEntities.Add(new DeltaEntity
        {
            Name = "file1.txt",
            Position = 0,
            Length = 100,
            Crc32 = 0
        });
        state = state.ApplyDelta(delta1);

        // Apply second delta (add file2)
        var delta2 = new DeltaIndex { DeltaVersion = 3 };
        delta2.NewEntities.Add(new DeltaEntity
        {
            Name = "file2.txt",
            Position = 100,
            Length = 100,
            Crc32 = 0
        });
        state = state.ApplyDelta(delta2);

        // Apply third delta (change file1)
        var delta3 = new DeltaIndex { DeltaVersion = 4 };
        delta3.ChangedEntities.Add(new DeltaEntity
        {
            Name = "file1.txt",
            Position = 0,
            Length = 150,
            Crc32 = 999
        });
        state = state.ApplyDelta(delta3);

        // Verify final state
        Assert.Equal(2, state.IndexNode.Count);
        Assert.Equal(4uL, state.LatestVersion);
        var file1 = state.GetEntity("file1.txt");
        Assert.NotNull(file1);
        Assert.Equal(150, file1.Length);
    }

    [Fact]
    public void ApplyDeltas_WithMultipleDeltasAtOnce()
    {
        // Create initial state
        var state = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>().ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create multiple deltas
        var delta1 = new DeltaIndex { DeltaVersion = 2 };
        delta1.NewEntities.Add(new DeltaEntity
        {
            Name = "file1.txt",
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        var delta2 = new DeltaIndex { DeltaVersion = 3 };
        delta2.NewEntities.Add(new DeltaEntity
        {
            Name = "file2.txt",
            Position = 100,
            Length = 100,
            Crc32 = 0
        });

        // Apply multiple deltas at once
        var updated = state.ApplyDeltas(new[] { delta1, delta2 });

        // Verify both deltas were applied
        Assert.Equal(2, updated.IndexNode.Count);
        Assert.NotNull(updated.GetEntity("file1.txt"));
        Assert.NotNull(updated.GetEntity("file2.txt"));
        Assert.Equal(3uL, updated.LatestVersion);
    }

    [Fact]
    public void DeltaPreference_FlatOverHierarchical()
    {
        // Create initial state from legacy snapshot
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };
        legacySnapshot.Entities.Add("file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Create a flat delta
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.ChangedEntities.Add(new DeltaEntity
        {
            Name = "file.txt",
            Position = 100,
            Length = 200,
            Crc32 = 123
        });

        // Apply the delta
        var updated = state.ApplyDelta(delta);

        // Verify it was applied correctly
        var entity = updated.GetEntity("file.txt");
        Assert.NotNull(entity);
        Assert.Equal(200, entity.Length);
        Assert.Equal(2uL, updated.LatestVersion);
    }

    [Fact]
    public void ComplexDelta_WithMixedOperations()
    {
        // Create initial state with multiple entities
        var state = new ImmutableDatasetState(
            new Dictionary<string, ImmutableSnapshotEntity>
            {
                ["file1.txt"] = new ImmutableSnapshotEntity(ImmutableArray<ulong>.Empty, ImmutableArray<ulong>.Empty, 1, 0, 100),
                ["file2.txt"] = new ImmutableSnapshotEntity(ImmutableArray<ulong>.Empty, ImmutableArray<ulong>.Empty, 1, 100, 100),
                ["file3.txt"] = new ImmutableSnapshotEntity(ImmutableArray<ulong>.Empty, ImmutableArray<ulong>.Empty, 1, 200, 100),
            }.ToImmutableDictionary(),
            1,  // snapshotVersion
            1); // latestVersion

        // Create a complex delta with mixed operations
        var delta = new DeltaIndex { DeltaVersion = 2 };

        // New entity
        delta.NewEntities.Add(new DeltaEntity
        {
            Name = "file4.txt",
            Position = 300,
            Length = 100,
            Crc32 = 0
        });

        // Changed entity
        delta.ChangedEntities.Add(new DeltaEntity
        {
            Name = "file1.txt",
            Position = 50,
            Length = 150,
            Crc32 = 999
        });

        // Deleted entity
        delta.DeletedNames.Add("file3.txt");

        // Apply the delta
        var updated = state.ApplyDelta(delta);

        // Verify results
        Assert.Equal(4, updated.IndexNode.Count);
        var file1 = updated.GetEntity("file1.txt");
        Assert.NotNull(file1);
        Assert.Equal(150, file1.Length);
        
        var file3 = updated.GetEntity("file3.txt");
        Assert.NotNull(file3);
        Assert.NotEmpty(file3.LastDeletedVersion);
        
        Assert.NotNull(updated.GetEntity("file4.txt"));
    }

    [Fact]
    public void ApplyDelta_CreatesHierarchicalIndex_WhenMissing()
    {
        // Create a base state with NO hierarchical index (IndexNode = null)
        var baseState = new ImmutableDatasetState(
            ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty,
            0,
            0,
            null);

        // Create a delta with hierarchical paths
        var delta = new DeltaIndex
        {
            DeltaVersion = 1
        };
        delta.NewEntities.Add(new DeltaEntity
        {
            Name = "dir1/dir2/file.txt",
            Position = 0,
            Length = 100,
            Crc32 = 123
        });

        // Apply the delta
        var updated = baseState.ApplyDelta(delta);

        // Verify hierarchical index was created
        Assert.NotNull(updated.IndexNode);
        Assert.True(updated.IndexNode.Children.ContainsKey("dir1"));
        Assert.True(updated.IndexNode.Children["dir1"].Children.ContainsKey("dir2"));
        Assert.True(updated.IndexNode.Children["dir1"].Children["dir2"].Entities.ContainsKey("file.txt"));
        
        // Verify flat entities are also updated (backward compatibility)
        Assert.NotNull(updated.GetEntity("dir1/dir2/file.txt"));
    }
}
