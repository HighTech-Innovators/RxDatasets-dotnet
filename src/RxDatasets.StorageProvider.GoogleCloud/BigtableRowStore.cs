using System.Globalization;
using System.IO.Compression;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageProvider.GoogleCloud;

/// <summary>
/// One delta or snapshot, as stored in its Bigtable row. A payload is either inline (<see cref="Index"/>,
/// <see cref="Data"/>, possibly compressed per its encoding) or in the bucket (<see cref="IndexObject"/>,
/// <see cref="DataObject"/>). No data at all (a delta of only deletes) is <see cref="DataLength"/> 0.
/// </summary>
public sealed record BigtableArtifact(
    ulong Version,
    DateTimeOffset CreatedAt,
    long DataLength,
    byte[]? Index = null,
    byte[]? Data = null,
    string? IndexObject = null,
    string? DataObject = null,
    string? IndexEncoding = null,
    string? DataEncoding = null);

public enum BigtablePayload
{
    None,
    Index,
    Data
}

/// <summary>
/// The Bigtable table holding the deltas and snapshots of all datasets: one row per delta or snapshot, keyed
/// <c>{dataset prefix}{deltas|snapshots}/{version, 20 digits}</c>, so the rows of one kind sort by version. The rows are
/// the only record of which versions exist: there is no DeltaInfo or SnapshotInfo.
/// <para>
/// The table must exist, with the column families <c>meta</c> and <c>payload</c> (one version each): this library needs
/// only data permissions, so it does not create it.
/// </para>
/// </summary>
public sealed class BigtableRowStore
{
    public const string MetaFamily = "meta";
    public const string PayloadFamily = "payload";
    private const string CreatedAt = "created-at";
    private const string DataLength = "data-length";
    private const string IndexEncoding = "index-encoding";
    private const string DataEncoding = "data-encoding";
    private const string IndexObject = "index-object";
    private const string DataObject = "data-object";
    private const string IndexColumn = "index";
    private const string DataColumn = "data";
    public const string Brotli = "br";

    private readonly BigtableClient _client;
    private readonly TableName _tableName;

    public BigtableRowStore(IOptions<GoogleCloudConfig> config)
    {
        GoogleCloudConfig value = config.Value;
        _tableName = new TableName(
            value.ProjectId ?? throw new ArgumentException("ProjectId must be set to use Bigtable"),
            value.BigtableInstanceId ?? throw new ArgumentException("BigtableInstanceId must be set to use Bigtable"),
            value.BigtableTableId ?? throw new ArgumentException("BigtableTableId must be set to use Bigtable"));

        var builder = new BigtableClientBuilder();
        if (!string.IsNullOrEmpty(value.BigtableEmulatorHost))
        {
            builder.Endpoint = value.BigtableEmulatorHost;
            builder.ChannelCredentials = ChannelCredentials.Insecure;
        }
        else if (value.JsonCredentials != null)
        {
            builder.GoogleCredential = CredentialFactory.FromJson<GoogleCredential>(value.JsonCredentials);
        }

        _client = builder.Build();
    }

    /// <summary>Writes the row of a delta, unless that version exists: the commit point of a delta.</summary>
    /// <returns>False when the version exists already: another writer stored it.</returns>
    public async Task<bool> TryInsert(string prefix, ObjectNameTypeEnum type, BigtableArtifact artifact)
    {
        CheckAndMutateRowResponse response = await _client.CheckAndMutateRowAsync(_tableName,
            RowKey(prefix, type, artifact.Version), RowFilters.PassAllFilter(), trueMutations: [],
            falseMutations: ToMutations(artifact));
        return !response.PredicateMatched;
    }

    /// <summary>Writes the row of a snapshot, replacing it when it exists.</summary>
    public async Task Upsert(string prefix, ObjectNameTypeEnum type, BigtableArtifact artifact)
    {
        await _client.MutateRowAsync(_tableName, RowKey(prefix, type, artifact.Version),
            [Mutations.DeleteFromRow(), .. ToMutations(artifact)]);
    }

    public async Task<BigtableArtifact?> Read(string prefix, ObjectNameTypeEnum type, ulong version, BigtablePayload payload)
    {
        RowFilter filter = payload switch
        {
            BigtablePayload.None => RowFilters.FamilyNameExact(MetaFamily),
            _ => RowFilters.Interleave(
                RowFilters.FamilyNameExact(MetaFamily),
                RowFilters.Chain(RowFilters.FamilyNameExact(PayloadFamily),
                    RowFilters.ColumnQualifierExact(payload == BigtablePayload.Index ? IndexColumn : DataColumn)))
        };
        Row? row = await _client.ReadRowAsync(_tableName, RowKey(prefix, type, version), filter);
        return row == null ? null : Parse(row);
    }

    /// <summary>The rows of <paramref name="type"/> up to <paramref name="toVersion"/>, without their payloads.</summary>
    public async Task<List<BigtableArtifact>> List(string prefix, ObjectNameTypeEnum type, ulong toVersion = ulong.MaxValue)
    {
        RowSet rows = RowSet.FromRowRanges(RowRange.Closed(RowKey(prefix, type, 0), RowKey(prefix, type, toVersion)));
        return await _client.ReadRows(_tableName, rows, RowFilters.FamilyNameExact(MetaFamily))
            .Select(Parse)
            .ToListAsync();
    }

    /// <summary>
    /// The versions of <paramref name="type"/> from <paramref name="fromVersion"/> on, in order, read from the row keys
    /// only. (Not a reversed scan for the highest: this client library cannot do those.)
    /// </summary>
    public async Task<List<ulong>> Versions(string prefix, ObjectNameTypeEnum type, ulong fromVersion, long rowsLimit = 0)
    {
        RowSet rows = RowSet.FromRowRanges(
            RowRange.Closed(RowKey(prefix, type, fromVersion), RowKey(prefix, type, ulong.MaxValue)));
        RowFilter keysOnly = RowFilters.Chain(RowFilters.CellsPerRowLimit(1), RowFilters.StripValueTransformer());
        return await _client.ReadRows(_tableName, rows, keysOnly, rowsLimit)
            .Select(row => VersionOf(row.Key))
            .ToListAsync();
    }

    /// <summary>Deletes rows; at most 100k per call (Bigtable's limit for one MutateRows).</summary>
    public async Task Delete(string prefix, ObjectNameTypeEnum type, IReadOnlyCollection<ulong> versions)
    {
        if (versions.Count == 0)
            return;
        await _client.MutateRowsAsync(_tableName,
            versions.Select(v => Mutations.CreateEntry(RowKey(prefix, type, v), Mutations.DeleteFromRow())).ToArray());
    }

    /// <summary>Reads at most one row key: fails when Bigtable or the table cannot be reached.</summary>
    public async Task Ping()
    {
        RowFilter keysOnly = RowFilters.Chain(RowFilters.CellsPerRowLimit(1), RowFilters.StripValueTransformer());
        await _client.ReadRows(_tableName, rows: null, keysOnly, 1).ToListAsync(); // no row set: the whole table
    }

    public static string RowKeyPrefix(string prefix, ObjectNameTypeEnum type) =>
        prefix + (type == ObjectNameTypeEnum.Delta ? FolderNames.DeltasFolderName : FolderNames.SnapshotsFolderName) + "/";

    private static BigtableByteString RowKey(string prefix, ObjectNameTypeEnum type, ulong version) =>
        RowKeyPrefix(prefix, type) + version.ToString("D20", CultureInfo.InvariantCulture);

    private static ulong VersionOf(ByteString key)
    {
        string text = key.ToStringUtf8();
        return ulong.Parse(text[(text.LastIndexOf('/') + 1)..], CultureInfo.InvariantCulture);
    }

    private static IEnumerable<Mutation> ToMutations(BigtableArtifact artifact)
    {
        var version = new BigtableVersion(DateTime.UtcNow);
        Mutation Set(string family, string column, ByteString value) => Mutations.SetCell(family, column, value, version);
        Mutation SetText(string column, string value) => Set(MetaFamily, column, ByteString.CopyFromUtf8(value));

        yield return SetText(CreatedAt, artifact.CreatedAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        yield return SetText(DataLength, artifact.DataLength.ToString(CultureInfo.InvariantCulture));
        if (artifact.IndexEncoding != null)
            yield return SetText(IndexEncoding, artifact.IndexEncoding);
        if (artifact.DataEncoding != null)
            yield return SetText(DataEncoding, artifact.DataEncoding);
        if (artifact.IndexObject != null)
            yield return SetText(IndexObject, artifact.IndexObject);
        if (artifact.DataObject != null)
            yield return SetText(DataObject, artifact.DataObject);
        if (artifact.Index != null)
            yield return Set(PayloadFamily, IndexColumn, ByteString.CopyFrom(artifact.Index));
        if (artifact.Data != null)
            yield return Set(PayloadFamily, DataColumn, ByteString.CopyFrom(artifact.Data));
    }

    private static BigtableArtifact Parse(Row row)
    {
        Dictionary<(string, string), ByteString> cells = row.Families
            .SelectMany(f => f.Columns.Where(c => c.Cells.Count > 0)
                .Select(c => (Key: (f.Name, c.Qualifier.ToStringUtf8()), c.Cells[0].Value)))
            .ToDictionary(c => c.Key, c => c.Value);
        string? Text(string column) => cells.TryGetValue((MetaFamily, column), out ByteString? v) ? v.ToStringUtf8() : null;
        byte[]? Bytes(string column) => cells.TryGetValue((PayloadFamily, column), out ByteString? v) ? v.ToByteArray() : null;

        return new BigtableArtifact(
            VersionOf(row.Key),
            DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(Text(CreatedAt) ?? "0", CultureInfo.InvariantCulture)),
            long.Parse(Text(DataLength) ?? "0", CultureInfo.InvariantCulture),
            Bytes(IndexColumn),
            Bytes(DataColumn),
            Text(IndexObject),
            Text(DataObject),
            Text(IndexEncoding),
            Text(DataEncoding));
    }

    /// <summary>Brotli (fastest), when on and worth it: payloads under 10 KB, or that shrink less than 25%, stay as is.</summary>
    public static (byte[] Bytes, string? Encoding) Compress(byte[] payload, bool compress)
    {
        if (!compress || payload.Length < 10 * 1024)
            return (payload, null);

        using var output = new MemoryStream();
        using (var brotli = new BrotliStream(output, CompressionLevel.Fastest))
        {
            brotli.Write(payload);
        }

        byte[] compressed = output.ToArray();
        return compressed.Length <= payload.Length * 3L / 4 ? (compressed, Brotli) : (payload, null);
    }

    public static byte[] Decompress(byte[] payload, string? encoding)
    {
        if (encoding == null)
            return payload;
        if (encoding != Brotli)
            throw new NotSupportedException($"Unknown payload encoding {encoding}");

        using var input = new BrotliStream(new MemoryStream(payload), CompressionMode.Decompress);
        using var output = new MemoryStream();
        input.CopyTo(output);
        return output.ToArray();
    }
}
