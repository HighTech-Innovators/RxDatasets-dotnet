using Google.Protobuf;

namespace RxDatasets.Models;

/// <summary>
/// Converts between flat and nested (trie) representations of SnapshotIndex and DeltaIndex
/// for on-disk protobuf storage. Nested format eliminates redundant path prefix storage.
/// </summary>
public static class NestedIndexHelper
{
    /// <summary>
    /// Ensures a SnapshotIndex has its flat Entities field populated.
    /// If only nested data is present, flattens it into the flat map.
    /// Useful for code that iterates SnapshotIndex.Entities directly.
    /// </summary>
    public static void EnsureFlat(SnapshotIndex snapshotIndex)
    {
        if (snapshotIndex.Entities.Count > 0)
            return;

        if (snapshotIndex.Nested == null)
            return;

        FlattenNestedSnapshotRecursive(snapshotIndex.Nested, "", snapshotIndex.Entities);
    }

    private static void FlattenNestedSnapshotRecursive(
        SnapshotBucket bucket, string prefix, Google.Protobuf.Collections.MapField<string, SnapshotEntity> target)
    {
        foreach (var (segment, entity) in bucket.Leaves)
        {
            var fullName = prefix.Length == 0 ? segment : prefix + "/" + segment;
            target[fullName] = entity;
        }

        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            FlattenNestedSnapshotRecursive(child, childPrefix, target);
        }
    }

    #region SnapshotIndex

    /// <summary>
    /// Builds a SnapshotBucket trie from an EntityTrie for serialization.
    /// </summary>
    public static SnapshotBucket ToNestedSnapshot(EntityTrie trie)
    {
        var root = new SnapshotBucket();
        foreach (var (name, entity) in trie)
        {
            PlaceSnapshotEntity(root, name, entity.ToProto());
        }
        return root;
    }

    private static void PlaceSnapshotEntity(SnapshotBucket bucket, string fullName, SnapshotEntity entity)
    {
        var segments = fullName.Split('/');
        var current = bucket;

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

    /// <summary>
    /// Reads a SnapshotBucket trie into an EntityTrie.Builder.
    /// </summary>
    public static void ReadNestedSnapshot(SnapshotBucket bucket, EntityTrie.Builder builder)
    {
        ReadNestedSnapshotRecursive(bucket, "", builder);
    }

    private static void ReadNestedSnapshotRecursive(SnapshotBucket bucket, string prefix, EntityTrie.Builder builder)
    {
        foreach (var (segment, entity) in bucket.Leaves)
        {
            var fullName = prefix.Length == 0 ? segment : prefix + "/" + segment;
            builder.Add(fullName, ImmutableSnapshotEntity.FromProto(entity));
        }

        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            ReadNestedSnapshotRecursive(child, childPrefix, builder);
        }
    }

    #endregion

    /// <summary>
    /// Parses a DeltaIndex from bytes. If nested data is present, flattens it into the
    /// standard flat fields so all existing consumers work unchanged.
    /// </summary>
    public static DeltaIndex ParseDeltaIndex(ReadOnlySpan<byte> data)
    {
        var delta = DeltaIndex.Parser.ParseFrom(data);
        if (delta.Nested != null)
            FlattenDeltaIndex(delta);
        return delta;
    }

    /// <summary>
    /// Parses a DeltaIndex from a byte array. Overload for callers that have byte[].
    /// </summary>
    public static DeltaIndex ParseDeltaIndex(byte[] data)
    {
        var delta = DeltaIndex.Parser.ParseFrom(data);
        if (delta.Nested != null)
            FlattenDeltaIndex(delta);
        return delta;
    }

    /// <summary>
    /// Serializes a DeltaIndex using nested format. Clones the delta first so the
    /// original remains unchanged (callers may still read its flat fields).
    /// </summary>
    public static byte[] SerializeDeltaIndex(DeltaIndex delta)
    {
        var clone = delta.Clone();
        NestDeltaIndex(clone);
        return clone.ToByteArray();
    }

    #region DeltaIndex

    /// <summary>
    /// Nests flat DeltaIndex entity lists into a DeltaBucket trie.
    /// Moves entities from flat fields into the nested field, clearing the flat fields.
    /// </summary>
    public static void NestDeltaIndex(DeltaIndex delta)
    {
        var root = new DeltaBucket();

        foreach (var entity in delta.NewEntities)
            PlaceDeltaEntity(root, entity.Name, entity, DeltaEntityKind.New);

        foreach (var entity in delta.ChangedEntities)
            PlaceDeltaEntity(root, entity.Name, entity, DeltaEntityKind.Changed);

        foreach (var entity in delta.DeletedEntities)
            PlaceDeletedEntity(root, entity.Name, entity);

        foreach (var name in delta.DeletedNames)
            PlaceDeletedName(root, name);

        delta.NewEntities.Clear();
        delta.ChangedEntities.Clear();
        delta.DeletedEntities.Clear();
        delta.DeletedNames.Clear();
        delta.Nested = root;
    }

    /// <summary>
    /// Flattens nested DeltaBucket back into flat DeltaIndex fields.
    /// Called after parsing if nested data is present.
    /// </summary>
    public static void FlattenDeltaIndex(DeltaIndex delta)
    {
        if (delta.Nested == null)
            return;

        FlattenDeltaBucketRecursive(delta.Nested, "", delta);
        delta.Nested = null;
    }

    private static void FlattenDeltaBucketRecursive(DeltaBucket bucket, string prefix, DeltaIndex target)
    {
        foreach (var entity in bucket.NewEntities)
        {
            var fullName = prefix.Length == 0 ? entity.Name : prefix + "/" + entity.Name;
            target.NewEntities.Add(new DeltaEntity
            {
                Name = fullName,
                Position = entity.Position,
                Length = entity.Length,
                Crc32 = entity.Crc32,
                PreviousVersion = entity.PreviousVersion
            });
        }

        foreach (var entity in bucket.ChangedEntities)
        {
            var fullName = prefix.Length == 0 ? entity.Name : prefix + "/" + entity.Name;
            target.ChangedEntities.Add(new DeltaEntity
            {
                Name = fullName,
                Position = entity.Position,
                Length = entity.Length,
                Crc32 = entity.Crc32,
                PreviousVersion = entity.PreviousVersion
            });
        }

        foreach (var entity in bucket.DeletedEntities)
        {
            var fullName = prefix.Length == 0 ? entity.Name : prefix + "/" + entity.Name;
            target.DeletedEntities.Add(new DeletedEntity
            {
                Name = fullName,
                PreviousVersion = entity.PreviousVersion
            });
        }

        foreach (var name in bucket.DeletedNames)
        {
            var fullName = prefix.Length == 0 ? name : prefix + "/" + name;
            target.DeletedNames.Add(fullName);
        }

        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            FlattenDeltaBucketRecursive(child, childPrefix, target);
        }
    }

    private enum DeltaEntityKind { New, Changed }

    private static void PlaceDeltaEntity(DeltaBucket bucket, string fullName, DeltaEntity entity, DeltaEntityKind kind)
    {
        var segments = fullName.Split('/');
        var current = bucket;

        for (int i = 0; i < segments.Length - 1; i++)
        {
            if (!current.Children.TryGetValue(segments[i], out var child))
            {
                child = new DeltaBucket();
                current.Children[segments[i]] = child;
            }
            current = child;
        }

        var leafEntity = new DeltaEntity
        {
            Name = segments[^1],
            Position = entity.Position,
            Length = entity.Length,
            Crc32 = entity.Crc32,
            PreviousVersion = entity.PreviousVersion
        };

        if (kind == DeltaEntityKind.New)
            current.NewEntities.Add(leafEntity);
        else
            current.ChangedEntities.Add(leafEntity);
    }

    private static void PlaceDeletedEntity(DeltaBucket bucket, string fullName, DeletedEntity entity)
    {
        var segments = fullName.Split('/');
        var current = bucket;

        for (int i = 0; i < segments.Length - 1; i++)
        {
            if (!current.Children.TryGetValue(segments[i], out var child))
            {
                child = new DeltaBucket();
                current.Children[segments[i]] = child;
            }
            current = child;
        }

        current.DeletedEntities.Add(new DeletedEntity
        {
            Name = segments[^1],
            PreviousVersion = entity.PreviousVersion
        });
    }

    private static void PlaceDeletedName(DeltaBucket bucket, string fullName)
    {
        var segments = fullName.Split('/');
        var current = bucket;

        for (int i = 0; i < segments.Length - 1; i++)
        {
            if (!current.Children.TryGetValue(segments[i], out var child))
            {
                child = new DeltaBucket();
                current.Children[segments[i]] = child;
            }
            current = child;
        }

        current.DeletedNames.Add(segments[^1]);
    }

    #endregion
}
