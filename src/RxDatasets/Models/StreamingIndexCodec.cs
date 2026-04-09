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
        TrieNode? nestedRoot = null;
        int nestedCount = 0;
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
                    var bucketSpan = span.Slice(pos, len);
                    (nestedRoot, nestedCount, maxEnd) = ReadBucket(bucketSpan);
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
        if (nestedRoot != null)
        {
            var protoPattern = idPattern;
            var runtimePattern = compressor?.Pattern ?? "";

            if (protoPattern == runtimePattern)
            {
                trie = EntityTrie.FromRootNode(nestedRoot, nestedCount, compressor);
            }
            else
            {
                // Pattern transition — rare. Fall back to standard ParseFrom.
                var parsed = SnapshotIndex.Parser.ParseFrom(data.Span);
                var state = ImmutableDatasetState.FromProto(parsed, compressor);
                long transitionMaxEnd = 0;
                foreach (var (_, e) in state.Entities)
                {
                    long end = e.Position + e.Length;
                    if (end > transitionMaxEnd) transitionMaxEnd = end;
                }
                return new SnapshotReadResult(state, version, versionLegacy, transitionMaxEnd);
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
    /// Streaming read that builds a <see cref="SnapshotPositionLookup"/> directly from wire
    /// format without intermediate proto objects. Only tracks name→position for non-deleted entities.
    /// </summary>
    public static SnapshotPositionLookup ReadSnapshotPositionLookup(ReadOnlyMemory<byte> data)
    {
        var span = data.Span;
        int pos = 0;
        string idPattern = "";
        ulong version = 0, versionLegacy = 0;
        var positions = new Dictionary<string, long>();
        NameCompressor? expander = null;
        bool nestedReadBeforePattern = false;

        while (pos < span.Length)
        {
            int fieldNumber = ReadTag(span, ref pos, out int wireType);
            if (fieldNumber == 0) break;

            switch (fieldNumber)
            {
                case SI_IdPattern:
                    idPattern = ReadString(span, ref pos);
                    if (idPattern.Length > 0)
                        expander = NameCompressor.FromPattern(idPattern);
                    break;
                case SI_Version:
                    version = ReadVarint(span, ref pos);
                    break;
                case SI_VersionLegacy:
                    versionLegacy = ReadVarint(span, ref pos);
                    break;
                case SI_Nested:
                {
                    if (expander == null && idPattern.Length == 0)
                        nestedReadBeforePattern = true;
                    int len = (int)ReadVarint(span, ref pos);
                    var bucketSpan = span.Slice(pos, len);
                    ReadBucketPositions(bucketSpan, "", positions, expander);
                    pos += len;
                    break;
                }
                case SI_Entities:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    var entrySpan = span.Slice(pos, len);
                    ReadFlatPositionEntry(entrySpan, positions, expander);
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        // If nested data was read before id_pattern appeared (proto-serialized order),
        // positions contain compressed names that need expansion.
        if (nestedReadBeforePattern && expander != null)
        {
            var expanded = new Dictionary<string, long>(positions.Count);
            foreach (var (name, p) in positions)
                expanded[expander.Expand(name)] = p;
            positions = expanded;
        }

        var v = version > 0 ? version : versionLegacy;
        return SnapshotPositionLookup.FromPositions(v, positions);
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

    #region ── Position Lookup Reader ──

    private static void ReadBucketPositions(
        ReadOnlySpan<byte> span, string prefix, Dictionary<string, long> positions, NameCompressor? expander)
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
                    ReadLeafPosition(span.Slice(pos, len), prefix, positions, expander);
                    pos += len;
                    break;
                }
                case SB_Children:
                {
                    int len = (int)ReadVarint(span, ref pos);
                    ReadChildPositions(span.Slice(pos, len), prefix, positions, expander);
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
    }

    private static void ReadLeafPosition(
        ReadOnlySpan<byte> span, string prefix, Dictionary<string, long> positions, NameCompressor? expander)
    {
        int pos = 0;
        string? key = null;
        long position = 0;
        bool deleted = false;
        bool hasValue = false;

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
                    hasValue = true;
                    int len = (int)ReadVarint(span, ref pos);
                    (position, deleted) = ReadPositionOnly(span.Slice(pos, len));
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        if (!deleted && key != null && hasValue)
        {
            var fullName = prefix.Length > 0 ? prefix + "/" + key : key;
            if (expander != null) fullName = expander.Expand(fullName);
            positions[fullName] = position;
        }
    }

    private static void ReadChildPositions(
        ReadOnlySpan<byte> span, string prefix, Dictionary<string, long> positions, NameCompressor? expander)
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
                        ? (prefix.Length > 0 ? prefix + "/" + key : key)
                        : prefix;
                    ReadBucketPositions(span.Slice(pos, len), childPrefix, positions, expander);
                    pos += len;
                    break;
                }
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }
    }

    private static (long position, bool deleted) ReadPositionOnly(ReadOnlySpan<byte> span)
    {
        int pos = 0;
        ulong maxDeleted = 0, maxInserted = 0, lastUpdated = 0;
        long position = 0;

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
                default:
                    SkipField(span, ref pos, wireType);
                    break;
            }
        }

        ulong latestVersion = Math.Max(lastUpdated, Math.Max(maxDeleted, maxInserted));
        bool deleted = maxDeleted > 0 && latestVersion == maxDeleted;
        return (position, deleted);
    }

    private static void ReadFlatPositionEntry(
        ReadOnlySpan<byte> entrySpan, Dictionary<string, long> positions, NameCompressor? expander)
    {
        int pos = 0;
        string? key = null;
        long position = 0;
        bool deleted = false;
        bool hasValue = false;

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
                {
                    hasValue = true;
                    int len = (int)ReadVarint(entrySpan, ref pos);
                    (position, deleted) = ReadPositionOnly(entrySpan.Slice(pos, len));
                    pos += len;
                    break;
                }
                default:
                    SkipField(entrySpan, ref pos, wireType);
                    break;
            }
        }

        if (!deleted && key != null && hasValue)
        {
            var fullName = expander?.Expand(key) ?? key;
            positions[fullName] = position;
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
