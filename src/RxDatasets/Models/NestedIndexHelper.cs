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
    /// Reads a SnapshotBucket trie into an EntityTrie.Builder.
    /// Skips deleted entities — only alive entities are added to the trie.
    /// </summary>
    /// <summary>
    /// Reads a SnapshotBucket trie into an EntityTrie.Builder.
    /// When <paramref name="protoCompressor"/> is provided, names reconstructed from the
    /// nested structure are expanded to full form before being added to the builder
    /// (which may re-compress with its own, different pattern).
    /// </summary>
    public static void ReadNestedSnapshot(
        SnapshotBucket bucket, EntityTrie.Builder builder, NameCompressor? protoCompressor = null)
    {
        ReadNestedSnapshotRecursive(bucket, "", builder, protoCompressor);
    }

    private static void ReadNestedSnapshotRecursive(
        SnapshotBucket bucket, string prefix, EntityTrie.Builder builder, NameCompressor? protoCompressor)
    {
        foreach (var (segment, entity) in bucket.Leaves)
        {
            if (IndexEntity.IsDeletedInProto(entity))
                continue;
            var name = prefix.Length == 0 ? segment : prefix + "/" + segment;
            if (protoCompressor != null)
                name = protoCompressor.Expand(name);
            builder.Add(name, IndexEntity.FromProto(entity));
        }

        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            ReadNestedSnapshotRecursive(child, childPrefix, builder, protoCompressor);
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
        {
            var savedNested = delta.Nested; // Preserve for tree merge
            FlattenDeltaIndex(delta);       // Populates flat fields, clears Nested
            delta.Nested = savedNested;     // Restore: flat fields have expanded names, Nested has raw proto names
        }
        if (delta.IdPattern.Length > 0)
            ExpandDeltaNames(delta);
        return delta;
    }

    /// <summary>
    /// Parses a DeltaIndex from a byte array. Overload for callers that have byte[].
    /// </summary>
    public static DeltaIndex ParseDeltaIndex(byte[] data)
    {
        var delta = DeltaIndex.Parser.ParseFrom(data);
        if (delta.Nested != null)
        {
            var savedNested = delta.Nested;
            FlattenDeltaIndex(delta);
            delta.Nested = savedNested;
        }
        if (delta.IdPattern.Length > 0)
            ExpandDeltaNames(delta);
        return delta;
    }

    /// <summary>
    /// Serializes a DeltaIndex using nested format. Clones the delta first so the
    /// original remains unchanged (callers may still read its flat fields).
    /// When a <see cref="NameCompressor"/> is provided, entity names are compressed
    /// before serialization and the pattern is stored in <c>id_pattern</c>.
    /// </summary>
    public static byte[] SerializeDeltaIndex(DeltaIndex delta, NameCompressor? compressor = null)
    {
        var clone = delta.Clone();

        if (compressor != null)
        {
            CompressDeltaNames(clone, compressor);
            clone.IdPattern = compressor.Pattern;
        }

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
                PreviousVersion = entity.PreviousVersion,
                TrackingId = entity.TrackingId,
                SlaTimestamp = entity.SlaTimestamp
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
                PreviousVersion = entity.PreviousVersion,
                TrackingId = entity.TrackingId,
                SlaTimestamp = entity.SlaTimestamp
            });
        }

        foreach (var entity in bucket.DeletedEntities)
        {
            var fullName = prefix.Length == 0 ? entity.Name : prefix + "/" + entity.Name;
            target.DeletedEntities.Add(new DeletedEntity
            {
                Name = fullName,
                PreviousVersion = entity.PreviousVersion,
                TrackingId = entity.TrackingId,
                SlaTimestamp = entity.SlaTimestamp
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
            PreviousVersion = entity.PreviousVersion,
            TrackingId = entity.TrackingId,
            SlaTimestamp = entity.SlaTimestamp
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
            PreviousVersion = entity.PreviousVersion,
            TrackingId = entity.TrackingId,
            SlaTimestamp = entity.SlaTimestamp
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

    #region ID Pattern compression

    /// <summary>
    /// Compresses all entity names in a DeltaIndex using the given <see cref="NameCompressor"/>.
    /// Called before nesting for on-disk storage.
    /// </summary>
    private static void CompressDeltaNames(DeltaIndex delta, NameCompressor compressor)
    {
        foreach (var entity in delta.NewEntities)
            entity.Name = compressor.Compress(entity.Name);
        foreach (var entity in delta.ChangedEntities)
            entity.Name = compressor.Compress(entity.Name);
        foreach (var entity in delta.DeletedEntities)
            entity.Name = compressor.Compress(entity.Name);

        for (int i = 0; i < delta.DeletedNames.Count; i++)
            delta.DeletedNames[i] = compressor.Compress(delta.DeletedNames[i]);
    }

    /// <summary>
    /// Expands all entity names in a DeltaIndex using the pattern stored in <c>id_pattern</c>.
    /// Called after parsing/flattening so all downstream consumers see full names.
    /// </summary>
    private static void ExpandDeltaNames(DeltaIndex delta)
    {
        var compressor = NameCompressor.FromPattern(delta.IdPattern);
        if (compressor == null) return;

        foreach (var entity in delta.NewEntities)
            entity.Name = compressor.Expand(entity.Name);
        foreach (var entity in delta.ChangedEntities)
            entity.Name = compressor.Expand(entity.Name);
        foreach (var entity in delta.DeletedEntities)
            entity.Name = compressor.Expand(entity.Name);

        for (int i = 0; i < delta.DeletedNames.Count; i++)
            delta.DeletedNames[i] = compressor.Expand(delta.DeletedNames[i]);
    }

    /// <summary>
    /// Expands entity name keys in a SnapshotIndex after flattening, when the proto
    /// was written with a compressed <c>id_pattern</c>.
    /// Rebuilds the <see cref="SnapshotIndex.Entities"/> map with expanded keys.
    /// </summary>
    internal static void ExpandSnapshotNames(SnapshotIndex snapshotIndex)
    {
        var compressor = NameCompressor.FromPattern(snapshotIndex.IdPattern);
        if (compressor == null) return;

        var expandedEntries = snapshotIndex.Entities
            .Select(kvp => (Key: compressor.Expand(kvp.Key), Value: kvp.Value))
            .ToList();

        snapshotIndex.Entities.Clear();
        foreach (var (key, value) in expandedEntries)
            snapshotIndex.Entities[key] = value;
    }

    #endregion
}
