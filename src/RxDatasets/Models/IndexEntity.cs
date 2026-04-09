namespace RxDatasets.Models;

/// <summary>
/// Slim in-memory representation of an entity in the index trie.
/// Unlike <see cref="ImmutableSnapshotEntity"/>, this holds no version-history arrays —
/// just the minimum needed for reads and writes. Presence in the trie means alive;
/// deleted entities are removed from the trie entirely.
/// </summary>
public record IndexEntity(ulong LatestVersion, long Position, int Length, uint Crc32 = 0)
{
    public static IndexEntity FromProto(SnapshotEntity proto)
    {
        var latestVersion = Math.Max(proto.LastUpdatedVersion,
            Math.Max(
                proto.LastDeletedVersion.Count > 0 ? proto.LastDeletedVersion[^1] : 0,
                proto.LastInsertedVersion.Count > 0 ? proto.LastInsertedVersion[^1] : 0));
        return new IndexEntity(latestVersion, proto.Position, proto.Length, proto.Crc32);
    }

    /// <summary>
    /// Determines whether the proto entity's latest change is a deletion.
    /// Used during snapshot loading to skip deleted entities (they should not be in the trie).
    /// </summary>
    public static bool IsDeletedInProto(SnapshotEntity proto)
    {
        var latestVersion = Math.Max(proto.LastUpdatedVersion,
            Math.Max(
                proto.LastDeletedVersion.Count > 0 ? proto.LastDeletedVersion[^1] : 0,
                proto.LastInsertedVersion.Count > 0 ? proto.LastInsertedVersion[^1] : 0));

        return proto.LastDeletedVersion.Count > 0 && latestVersion == proto.LastDeletedVersion[^1];
    }
}
