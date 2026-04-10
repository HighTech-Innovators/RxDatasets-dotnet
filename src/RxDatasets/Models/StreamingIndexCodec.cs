using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Runtime.InteropServices;
using System.Text;
using Google.Protobuf;

namespace RxDatasets.Models;

/// <summary>
/// Streaming protobuf codec for SnapshotIndex and DeltaIndex.
///
/// Reader: walks the proto wire format using zero-copy span slicing and builds EntityTrie
/// nodes directly from the byte stream — no intermediate proto objects.
/// For a 545 MB snapshot index this avoids ~3-5× proto object graph overhead.
///
/// Writer: serializes with <c>id_pattern</c> as the FIRST field so streaming readers know
/// the compression pattern before reading entity names.
/// </summary>
internal static class StreamingIndexCodec
{
    // ── SnapshotIndex field numbers ──
    private const int SI_VersionLegacy = 1;  // uint64 (varint)
    private const int SI_Entities      = 2;  // map<string, SnapshotEntity> (repeated LEN)
    private const int SI_Version       = 32; // uint64 (varint)
    private const int SI_Nested        = 33; // SnapshotBucket (LEN)
    private const int SI_IdPattern     = 34; // string (LEN)

    // ── SnapshotBucket field numbers ──
    private const int SB_Leaves   = 1; // map<string, SnapshotEntity>
    private const int SB_Children = 2; // map<string, SnapshotBucket>

    // ── SnapshotEntity field numbers ──
    private const int SE_LastDeletedVersion  = 1; // repeated uint64
    private const int SE_LastInsertedVersion = 2; // repeated uint64
    private const int SE_LastUpdatedVersion  = 3; // uint64
    private const int SE_Position            = 4; // int64
    private const int SE_Length              = 5; // int32
    private const int SE_Crc32              = 7; // uint32

    // ── Map entry field numbers (standard protobuf) ──
    private const int Map_Key   = 1;
    private const int Map_Value = 2;

    // Protobuf wire types
    private const int WireVarint = 0;
    private const int Wire64Bit = 1;
    private const int WireLengthDelimited = 2;
    private const int Wire32Bit = 5;

    #region ── Snapshot Writing ──

    /// <summary>
    /// Serializes a snapshot to bytes with <c>id_pattern</c> written FIRST.
    /// The nested tree is written using the proto object's own <see cref="IMessage.WriteTo"/>.
    /// </summary>
    public static byte[] WriteSnapshot(ulong version, string idPattern, SnapshotBucket? nested)
    {
        int size = 0;

        if (idPattern.Length > 0)
            size += CodedOutputStream.ComputeTagSize(SI_IdPattern) + CodedOutputStream.ComputeStringSize(idPattern);

        if (version > 0)
            size += CodedOutputStream.ComputeTagSize(SI_Version) + CodedOutputStream.ComputeUInt64Size(version);

        int bucketSize = 0;
        if (nested != null)
        {
            bucketSize = nested.CalculateSize();
            size += CodedOutputStream.ComputeTagSize(SI_Nested) + CodedOutputStream.ComputeLengthSize(bucketSize) + bucketSize;
        }

        var buffer = new byte[size];
        var output = new CodedOutputStream(buffer);

        if (idPattern.Length > 0)
        {
            output.WriteTag(SI_IdPattern, WireFormat.WireType.LengthDelimited);
            output.WriteString(idPattern);
        }

        if (version > 0)
        {
            output.WriteTag(SI_Version, WireFormat.WireType.Varint);
            output.WriteUInt64(version);
        }

        if (nested != null)
        {
            output.WriteTag(SI_Nested, WireFormat.WireType.LengthDelimited);
            output.WriteLength(bucketSize);
            nested.WriteTo(output);
        }

        output.CheckNoSpaceLeft();
        return buffer;
    }

    #endregion

    #region ── Delta Writing ──

    /// <summary>
    /// Serializes a DeltaIndex with <c>id_pattern</c> written FIRST.
    /// The rest of the fields are written using the proto's own <see cref="IMessage.WriteTo"/>.
    /// </summary>
    public static byte[] WriteDelta(DeltaIndex delta)
    {
        string idPattern = delta.IdPattern;
        delta.IdPattern = ""; // Clear so WriteTo skips it

        int size = 0;
        if (idPattern.Length > 0)
            size += CodedOutputStream.ComputeTagSize(34) + CodedOutputStream.ComputeStringSize(idPattern);
        size += delta.CalculateSize();

        var buffer = new byte[size];
        var output = new CodedOutputStream(buffer);

        if (idPattern.Length > 0)
        {
            output.WriteTag(34, WireFormat.WireType.LengthDelimited);
            output.WriteString(idPattern);
        }

        delta.WriteTo(output);
        output.CheckNoSpaceLeft();

        delta.IdPattern = idPattern; // Restore
        return buffer;
    }

    #endregion

    #region ── Snapshot Reading ──

    /// <summary>
    /// Result of a streaming snapshot read — includes validation info alongside the state.
    /// </summary>
    internal readonly record struct SnapshotReadResult(
        ImmutableDatasetState State, ulong Version, ulong VersionLegacy, long MaxEndPosition);

    /// <summary>
    /// Reads a SnapshotIndex from raw bytes by streaming through the proto wire format.
    /// Builds EntityTrie nodes directly — no intermediate proto objects.
    /// Returns the trie, raw version fields, and the max (position + length) for data-file validation.
    /// </summary>
    public static SnapshotReadResult ReadSnapshot(
        ReadOnlyMemory<byte> data, NameCompressor? compressor = null)
    {
        var span = data.Span;
        int pos = 0;

        string idPattern = "";
        ulong version = 0, versionLegacy = 0;
        ReadOnlySpan<byte> nestedSpan = default;
        long maxEnd = 0;
        EntityTrie.Builder? legacyBuilder = null;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case SI_IdPattern:
                    idPattern = ReadString(span, ref pos);
                    if (compressor == null && idPattern.Length > 0)
                        compressor = NameCompressor.FromPattern(idPattern);
                    break;

                case SI_Version:
                    version = ReadVarint(span, ref pos);
                    break;

                case SI_VersionLegacy:
                    versionLegacy = ReadVarint(span, ref pos);
                    break;

                case SI_Nested:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    nestedSpan = span.Slice(pos, len);
                    pos += len;
                    break;
                }

                case SI_Entities:
                {
                    // Legacy flat map — each occurrence is one map entry
                    legacyBuilder ??= new EntityTrie.Builder(compressor);
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    ReadFlatSnapshotMapEntry(entrySpan, legacyBuilder,
                        idPattern.Length > 0 && idPattern != (compressor?.Pattern ?? "")
                            ? NameCompressor.FromPattern(idPattern) : null,
                        ref maxEnd);
                    pos += len;
                    break;
                }

                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        EntityTrie trie;
        if (nestedSpan.Length > 0)
        {
            var protoPattern = idPattern;
            var runtimePattern = compressor?.Pattern ?? "";

            if (protoPattern == runtimePattern)
            {
                // Patterns match — build frozen trie directly from wire format (fast path)
                var (nestedRoot, nestedCount, nestedMaxEnd) = ReadBucket(nestedSpan);
                maxEnd = nestedMaxEnd;
                trie = EntityTrie.FromRootNode(nestedRoot, nestedCount, compressor);
            }
            else
            {
                // Pattern transition: stream wire format directly into a Builder
                // with the target compressor. Never materializes the intermediate frozen trie.
                var sourceExpander = protoPattern.Length > 0
                    ? NameCompressor.FromPattern(protoPattern) : null;
                var builder = new EntityTrie.Builder(compressor);
                ReadBucketIntoBuilder(nestedSpan, "", sourceExpander, builder, ref maxEnd);
                trie = builder.Build();
            }
        }
        else if (legacyBuilder != null)
        {
            trie = legacyBuilder.Build();
        }
        else
        {
            trie = EntityTrie.Empty;
        }

        var v = version > 0 ? version : versionLegacy;
        return new SnapshotReadResult(
            new ImmutableDatasetState(trie, v, v), version, versionLegacy, maxEnd);
    }

    /// <summary>
    /// Streams through a SnapshotBucket's wire format depth-first, reading each leaf entity
    /// and inserting it directly into the target Builder. Constructs full source-format names
    /// from the prefix path, expands them if the source had compression, and lets the Builder
    /// compress for the target pattern. Never materializes a frozen intermediate trie.
    /// </summary>
    private static void ReadBucketIntoBuilder(
        ReadOnlySpan<byte> span, string prefix, NameCompressor? sourceExpander,
        EntityTrie.Builder builder, ref long maxEnd)
    {
        int pos = 0;
        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case SB_Leaves:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    ReadLeafIntoBuilder(entrySpan, prefix, sourceExpander, builder, ref maxEnd);
                    pos += len;
                    break;
                }
                case SB_Children:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    ReadChildIntoBuilder(entrySpan, prefix, sourceExpander, builder, ref maxEnd);
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
    }

    private static void ReadLeafIntoBuilder(
        ReadOnlySpan<byte> span, string prefix, NameCompressor? sourceExpander,
        EntityTrie.Builder builder, ref long maxEnd)
    {
        int pos = 0;
        string? key = null;
        IndexEntity? entity = null;
        bool deleted = false;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case Map_Key:
                    key = ReadString(span, ref pos);
                    break;
                case Map_Value:
                    int len = (int)ReadVarint(span, ref pos);
                    (entity, deleted) = ReadSnapshotEntityFields(span.Slice(pos, len));
                    pos += len;
                    break;
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        if (!deleted && key != null && entity != null)
        {
            var compressedPath = prefix.Length > 0 ? $"{prefix}/{key}" : key;
            var fullName = sourceExpander?.Expand(compressedPath) ?? compressedPath;
            builder.Add(fullName, entity);
            long end = entity.Position + entity.Length;
            if (end > maxEnd) maxEnd = end;
        }
    }

    private static void ReadChildIntoBuilder(
        ReadOnlySpan<byte> span, string prefix, NameCompressor? sourceExpander,
        EntityTrie.Builder builder, ref long maxEnd)
    {
        int pos = 0;
        string? key = null;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case Map_Key:
                    key = ReadString(span, ref pos);
                    break;
                case Map_Value:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var childPrefix = key != null
                        ? (prefix.Length > 0 ? $"{prefix}/{key}" : key)
                        : prefix;
                    ReadBucketIntoBuilder(
                        span.Slice(pos, len), childPrefix, sourceExpander, builder, ref maxEnd);
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
    }

    #endregion

    #region ── Bucket Reader (builds TrieNode directly) ──

    private static (TrieNode root, int count, long maxEnd) ReadBucket(ReadOnlySpan<byte> span)
    {
        int pos = 0;
        var children = new Dictionary<string, TrieNode>();
        var leaves = new Dictionary<string, IndexEntity>();
        int count = 0;
        long maxEnd = 0;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case SB_Leaves:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    var (key, entity, deleted) = ReadLeafEntry(entrySpan);
                    pos += len;

                    if (!deleted && key != null && entity != null)
                    {
                        leaves[key] = entity;
                        count++;
                        long end = entity.Position + entity.Length;
                        if (end > maxEnd) maxEnd = end;
                    }
                    break;
                }

                case SB_Children:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    var (key, child, childCount, childMaxEnd) = ReadChildEntry(entrySpan);
                    pos += len;

                    if (key != null && child != TrieNode.EmptyNode)
                    {
                        children[key] = child;
                        count += childCount;
                        if (childMaxEnd > maxEnd) maxEnd = childMaxEnd;
                    }
                    break;
                }

                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        // Merge: a segment can be both a child (intermediate) and a leaf (entity)
        var dict = new Dictionary<string, TrieNode>(children.Count + leaves.Count);
        foreach (var (k, v) in children) dict[k] = v;
        foreach (var (k, entity) in leaves)
        {
            if (dict.TryGetValue(k, out var existing))
                dict[k] = new TrieNode(existing.Children, entity);
            else
                dict[k] = new TrieNode(FrozenDictionary<string, TrieNode>.Empty, entity);
        }

        return dict.Count == 0
            ? (TrieNode.EmptyNode, 0, 0)
            : (new TrieNode(dict.ToFrozenDictionary(), null), count, maxEnd);
    }

    private static (string? key, IndexEntity? entity, bool deleted) ReadLeafEntry(ReadOnlySpan<byte> span)
    {
        int pos = 0;
        string? key = null;
        IndexEntity? entity = null;
        bool deleted = false;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case Map_Key:
                    key = ReadString(span, ref pos);
                    break;
                case Map_Value:
                    int len = (int)ReadVarint(span, ref pos);
                    (entity, deleted) = ReadSnapshotEntityFields(span.Slice(pos, len));
                    pos += len;
                    break;
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
        return (key, entity, deleted);
    }

    private static (string? key, TrieNode node, int count, long maxEnd)
        ReadChildEntry(ReadOnlySpan<byte> span)
    {
        int pos = 0;
        string? key = null;
        TrieNode node = TrieNode.EmptyNode;
        int count = 0;
        long maxEnd = 0;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case Map_Key:
                    key = ReadString(span, ref pos);
                    break;
                case Map_Value:
                    int len = (int)ReadVarint(span, ref pos);
                    (node, count, maxEnd) = ReadBucket(span.Slice(pos, len));
                    pos += len;
                    break;
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
        return (key, node, count, maxEnd);
    }

    /// <summary>
    /// Reads SnapshotEntity fields directly from wire format into an IndexEntity.
    /// Handles both packed and unpacked repeated uint64 fields.
    /// </summary>
    private static (IndexEntity entity, bool deleted) ReadSnapshotEntityFields(ReadOnlySpan<byte> span)
    {
        int pos = 0;
        ulong maxDeleted = 0, maxInserted = 0, lastUpdated = 0;
        long position = 0;
        int length = 0;
        uint crc32 = 0;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case SE_LastDeletedVersion:
                    ReadPackedOrSingleUInt64(span, ref pos, wireType, ref maxDeleted);
                    break;
                case SE_LastInsertedVersion:
                    ReadPackedOrSingleUInt64(span, ref pos, wireType, ref maxInserted);
                    break;
                case SE_LastUpdatedVersion:
                    lastUpdated = ReadVarint(span, ref pos);
                    break;
                case SE_Position:
                    position = (long)ReadVarint(span, ref pos);
                    break;
                case SE_Length:
                    length = (int)ReadVarint(span, ref pos);
                    break;
                case SE_Crc32:
                    crc32 = (uint)ReadVarint(span, ref pos);
                    break;
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        ulong latestVersion = Math.Max(lastUpdated, Math.Max(maxDeleted, maxInserted));
        bool deleted = maxDeleted > 0 && latestVersion == maxDeleted;
        return (new IndexEntity(latestVersion, position, length, crc32), deleted);
    }

    private static void ReadPackedOrSingleUInt64(
        ReadOnlySpan<byte> span, ref int pos, int wireType, ref ulong maxValue)
    {
        if (wireType == WireLengthDelimited)
        {
            int len = (int)ReadVarint(span, ref pos);
            int end = pos + len;
            while (pos < end)
            {
                ulong v = ReadVarint(span, ref pos);
                if (v > maxValue) maxValue = v;
            }
        }
        else
        {
            ulong v = ReadVarint(span, ref pos);
            if (v > maxValue) maxValue = v;
        }
    }

    #endregion

    #region ── Legacy Flat Map Reader ──

    private static void ReadFlatSnapshotMapEntry(
        ReadOnlySpan<byte> entrySpan, EntityTrie.Builder builder,
        NameCompressor? flatExpander, ref long maxEnd)
    {
        int pos = 0;
        string? key = null;
        IndexEntity? entity = null;
        bool deleted = false;

        while (pos < entrySpan.Length)
        {
            int fieldNumber = ReadTag(entrySpan, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case Map_Key:
                    key = ReadString(entrySpan, ref pos);
                    break;
                case Map_Value:
                    int len = (int)ReadVarint(entrySpan, ref pos);
                    (entity, deleted) = ReadSnapshotEntityFields(entrySpan.Slice(pos, len));
                    pos += len;
                    break;
                default:
                    SkipField(entrySpan, ref pos, wireType);
                    break;
            }
        }

        if (!deleted && key != null && entity != null)
        {
            var name = flatExpander != null ? flatExpander.Expand(key) : key;
            builder.Add(name, entity);
            long end = entity.Position + entity.Length;
            if (end > maxEnd) maxEnd = end;
        }
    }

    #endregion

    #region ── Protobuf Wire Format Primitives ──

    private static ulong ReadVarint(ReadOnlySpan<byte> span, ref int pos)
    {
        ulong result = 0;
        int shift = 0;
        while (pos < span.Length)
        {
            byte b = span[pos++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
        return result;
    }

    private static int ReadTag(ReadOnlySpan<byte> span, ref int pos, out int wireType)
    {
        if (pos >= span.Length) { wireType = 0; return 0; }
        ulong tag = ReadVarint(span, ref pos);
        wireType = (int)(tag & 0x7);
        return (int)(tag >> 3);
    }

    private static string ReadString(ReadOnlySpan<byte> span, ref int pos)
    {
        int len = (int)ReadVarint(span, ref pos);
        if (len == 0) { return ""; }
        var str = Encoding.UTF8.GetString(span.Slice(pos, len));
        pos += len;
        return str;
    }

    private static void SkipField(ReadOnlySpan<byte> span, ref int pos, int wireType)
    {
        switch (wireType)
        {
            case WireVarint:
                while (pos < span.Length && (span[pos++] & 0x80) != 0) { }
                break;
            case Wire64Bit:
                pos += 8;
                break;
            case WireLengthDelimited:
                int len = (int)ReadVarint(span, ref pos);
                pos += len;
                break;
            case Wire32Bit:
                pos += 4;
                break;
        }
    }

    #endregion
}
