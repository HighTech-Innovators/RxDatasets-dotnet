using System.Collections.Immutable;
using RxDatasets.Models;

namespace RxDatasets.Tests.Compatibility;

/// <summary>
/// Integration tests for backward compatibility with legacy snapshot files.
/// Verifies that the system can read and work with old snapshot files.
/// </summary>
public class LegacySnapshotTests
{
    private ImmutableSnapshotEntity CreateTestEntity(long position = 0, int length = 100)
    {
        return new ImmutableSnapshotEntity(
            ImmutableArray<ulong>.Empty,
            ImmutableArray<ulong>.Empty,
            1,
            position,
            length);
    }

    [Fact]
    public void LegacySnapshot_FlatEntities_CanBeLoaded()
    {
        // Create a legacy snapshot with only flat entities (no hierarchical root)
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };

        legacySnapshot.Entities.Add("entity1", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        legacySnapshot.Entities.Add("entity2", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 100,
            Length = 200,
            Crc32 = 0
        });

        // Load the legacy snapshot
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify all entities are loaded
        Assert.Equal(2, state.Entities.Count);
        Assert.Contains("entity1", state.Entities.Keys);
        Assert.Contains("entity2", state.Entities.Keys);
    }

    [Fact]
    public void LegacySnapshot_WithPathLikeKeys_PreservesFullPath()
    {
        // Create a legacy snapshot with path-like entity keys
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };

        legacySnapshot.Entities.Add("dir1/dir2/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        legacySnapshot.Entities.Add("dir1/file2.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 100,
            Length = 50,
            Crc32 = 0
        });

        // Load the legacy snapshot
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify paths are preserved as-is
        Assert.Equal(2, state.Entities.Count);
        Assert.Contains("dir1/dir2/file.txt", state.Entities.Keys);
        Assert.Contains("dir1/file2.txt", state.Entities.Keys);
    }

    [Fact]
    public void LegacySnapshot_DataAccess_Works()
    {
        // Create a legacy snapshot
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };

        var entity = new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 500,
            Length = 250,
            Crc32 = 12345
        };
        legacySnapshot.Entities.Add("data/file.dat", entity);

        // Load and verify data access
        var state = ImmutableDatasetState.FromProto(legacySnapshot);
        var loaded = state.Entities["data/file.dat"];

        Assert.Equal(500L, loaded.Position);
        Assert.Equal(250, loaded.Length);
        Assert.Equal(12345u, loaded.Crc32);
    }

    [Fact]
    public void LegacySnapshot_VersionTracking_Preserved()
    {
        // Create a legacy snapshot with version info
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersionLegacy = 10,
            SnapshotVersion = 20  // Preferred version
        };

        legacySnapshot.Entities.Add("entity", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { 5, 10 },
            LastUpdatedVersion = 8,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Load the snapshot
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify version is set to the preferred version
        Assert.Equal(20uL, state.SnapshotVersion);
        Assert.Equal(20uL, state.LatestVersion);

        // Verify entity version history is preserved
        var entity = state.Entities["entity"];
        Assert.Contains(5uL, entity.LastInsertedVersion);
        Assert.Contains(10uL, entity.LastInsertedVersion);
        Assert.Equal(8uL, entity.LastUpdatedVersion);
    }

    [Fact]
    public void LegacySnapshot_LargeDataset_Handles()
    {
        // Create a legacy snapshot with many entities (simulating large dataset)
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };

        for (int i = 0; i < 1000; i++)
        {
            legacySnapshot.Entities.Add($"file_{i:D6}.dat", new SnapshotEntity
            {
                LastDeletedVersion = { },
                LastInsertedVersion = { },
                LastUpdatedVersion = 1,
                Position = i * 1000L,
                Length = 1000,
                Crc32 = (uint)i
            });
        }

        // Load the snapshot
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify all entities loaded
        Assert.Equal(1000, state.Entities.Count);
        Assert.NotEmpty(state.Entities);
    }

    [Fact]
    public void HierarchicalSnapshot_PreferredOverLegacy()
    {
        // Create a snapshot with both legacy and new structures
        var hierarchicalRoot = new SnapshotNode();
        var hierarchicalChild = new SnapshotNode();
        hierarchicalChild.Entities.Add("newfile.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 2,
            Position = 0,
            Length = 50,
            Crc32 = 0
        });
        hierarchicalRoot.Children.Add("newdir", hierarchicalChild);

        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 2,
            Root = hierarchicalRoot
        };

        // Add some legacy entities (should be ignored)
        snapshotIndex.Entities.Add("legacy/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Load - should prefer the hierarchical root
        var state = ImmutableDatasetState.FromProto(snapshotIndex);

        // The flat view should come from the hierarchical root, not legacy
        var flatEntities = state.GetFlatEntities();
        Assert.Contains("newdir/newfile.txt", flatEntities.Keys);
    }

    [Fact]
    public void LegacyAndHierarchical_CanInteroperate()
    {
        // Create initial state from legacy snapshot
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };
        legacySnapshot.Entities.Add("legacy/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify it loads correctly as flat
        Assert.Contains("legacy/file.txt", state.Entities.Keys);

        // Now serialize and verify it's still valid
        var serialized = state.ToProto();
        var reloaded = ImmutableDatasetState.FromProto(serialized);

        // Should still have the data
        Assert.NotEmpty(reloaded.Entities);
    }
}
