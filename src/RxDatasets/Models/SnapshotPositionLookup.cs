namespace RxDatasets.Models;

/// <summary>
/// Lightweight position lookup for the most-recent snapshot, replacing the full
/// <see cref="ImmutableDatasetState"/> that was previously kept as a second copy
/// of the entire entity trie. Stores only {name → position} for non-deleted entities.
/// </summary>
public sealed class SnapshotPositionLookup
{
    public ulong SnapshotVersion { get; }
    private readonly Dictionary<string, long> _positions;

    private SnapshotPositionLookup(ulong snapshotVersion, Dictionary<string, long> positions)
    {
        SnapshotVersion = snapshotVersion;
        _positions = positions;
    }

    public bool TryGetPosition(string name, out long position)
        => _positions.TryGetValue(name, out position);

    public static SnapshotPositionLookup FromProto(SnapshotIndex proto)
    {
        var positions = new Dictionary<string, long>();

        if (proto.Nested is not null && proto.Nested.Children.Count > 0)
        {
            ReadPositionsFromNested(proto.Nested, "", positions);
        }
        else
        {
            foreach (var (name, entity) in proto.Entities)
            {
                if (!IndexEntity.IsDeletedInProto(entity))
                    positions[name] = entity.Position;
            }
        }

        return new SnapshotPositionLookup(proto.SnapshotVersion, positions);
    }

    private static void ReadPositionsFromNested(SnapshotBucket bucket, string prefix, Dictionary<string, long> positions)
    {
        foreach (var (leafName, entity) in bucket.Leaves)
        {
            if (!IndexEntity.IsDeletedInProto(entity))
            {
                var fullName = prefix.Length > 0 ? prefix + "/" + leafName : leafName;
                positions[fullName] = entity.Position;
            }
        }

        foreach (var (childName, childBucket) in bucket.Children)
        {
            var childPrefix = prefix.Length > 0 ? prefix + "/" + childName : childName;
            ReadPositionsFromNested(childBucket, childPrefix, positions);
        }
    }
}
