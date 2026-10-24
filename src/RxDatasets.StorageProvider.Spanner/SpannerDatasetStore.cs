using System.IO.Pipelines;
using System.Text.RegularExpressions;
using Google.Cloud.Spanner.Data;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageProvider.Spanner;

/// <summary>
/// A dataset's deltas and snapshots as Spanner rows. The version row, keyed (dataset, kind, version), is the commit
/// point: it is inserted, never upserted, so a second writer of the same version fails, and the available versions are
/// a query, so there is no DeltaInfo/SnapshotInfo file to keep in step. The index and data bytes are chunk rows of a
/// fresh blob id that the version row points at, so blobs of any size fit under the cell and commit limits, and a writer
/// that loses the race (or crashes) never touches the winner's chunks.
/// </summary>
public class SpannerDatasetStore<T> : IStorageReader<T>
{
    // ponytail: fixed sizes. A cell holds at most 10 MiB and a commit 100 MB. Reads cut their range server side
    // (SUBSTR), so the chunk size only sets the write batches and the streaming granularity.
    private const int ChunkSize = 1024 * 1024;
    private const int ChunksPerCommit = 32;
    private const long DeltaKind = 0;
    private const long SnapshotKind = 1;
    private const long IndexPart = 0;
    private const long DataPart = 1;

    private readonly string _connectionString;
    private readonly string _dataset;
    private readonly string _versions;
    private readonly string _chunks;
    private readonly string _processed;
    private Task? _schema;

    public SpannerDatasetStore(IOptions<SpannerConfig> config, IOptions<StorageConfig> storageConfig)
    {
        _connectionString = config.Value.ConnectionString;
        _dataset = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);
        var prefix = config.Value.TablePrefix;
        if (config.Value.TablePerDataset)
        {
            prefix += "_" + Regex.Replace(_dataset, "[^A-Za-z0-9]+", "_").Trim('_');
        }

        _versions = prefix + "_Versions";
        _chunks = prefix + "_Chunks";
        _processed = prefix + "_ProcessedVersions";
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        await Ready();
        await using var connection = await Open();
        // One query, so one consistent read of the delta range and the snapshots
        await using var command = connection.CreateSelectCommand(
            $"SELECT (SELECT Version FROM {_versions} WHERE Dataset = @dataset AND Kind = 0 ORDER BY Version LIMIT 1), " +
            $"(SELECT Version FROM {_versions} WHERE Dataset = @dataset AND Kind = 0 ORDER BY Version DESC LIMIT 1), " +
            $"ARRAY(SELECT Version FROM {_versions} WHERE Dataset = @dataset AND Kind = 1 ORDER BY Version), " +
            $"ARRAY(SELECT CreatedAt FROM {_versions} WHERE Dataset = @dataset AND Kind = 1 ORDER BY Version)",
            Parameters());
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();

        var snapshots = reader.GetFieldValue<List<long>>(2)
            .Zip(reader.GetFieldValue<List<DateTime>>(3),
                (version, createdAt) => new SnapshotType((ulong)version, new DateTimeOffset(createdAt.ToUniversalTime())))
            .ToList();
        // Cleaning can remove every delta up to the latest snapshot; the dataset is then at that snapshot's version
        var newest = Math.Max(reader.IsDBNull(1) ? 0 : (ulong)reader.GetInt64(1), snapshots.Count > 0 ? snapshots[^1].Version : 0);
        var oldest = !reader.IsDBNull(0) ? (ulong)reader.GetInt64(0) : newest == 0 ? 0 : newest + 1;
        return (oldest, newest, snapshots);
    }

    /// <returns>False when the version was already stored, by another writer.</returns>
    public Task<bool> StoreDelta(ulong version, byte[] index, Stream data) => Store(DeltaKind, version, index, data);

    /// <returns>False when the version was already stored, by another writer.</returns>
    public Task<bool> StoreSnapshot(ulong version, byte[] index, Stream data) => Store(SnapshotKind, version, index, data);

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version) => ReadPart(DeltaKind, version, IndexPart);

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version) => ReadPart(SnapshotKind, version, IndexPart);

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion) => ReadPart(SnapshotKind, snapshotVersion, DataPart);

    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback) =>
        ReadSnapshotDataAsync(snapshotVersion, stream =>
        {
            callback(stream);
            return Task.CompletedTask;
        });

    public async Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        var pipe = new Pipe();

        async Task Produce()
        {
            try
            {
                await CopyPart(SnapshotKind, snapshotVersion, DataPart, pipe.Writer.AsStream(true));
                await pipe.Writer.CompleteAsync();
            }
            catch (Exception e)
            {
                await pipe.Writer.CompleteAsync(e);
            }
        }

        var producer = Produce();
        await using (var stream = pipe.Reader.AsStream())
        {
            await asyncCallback(stream);
        }

        await producer;
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
        {
            throw new ArgumentException("at least one source must be filled");
        }

        if (length == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        foreach (var (type, version, position) in sources)
        {
            var bytes = await ReadRange(type == ObjectNameTypeEnum.Snapshot ? SnapshotKind : DeltaKind, version, position, length);
            if (bytes is not null)
            {
                return bytes;
            }
        }

        throw new FileNotFoundException($"None of the {sources.Count} sources is in {_versions}", _dataset);
    }

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        await using var connection = await Open();
        await using var command = connection.CreateSelectCommand(
            $"SELECT DataLength FROM {_versions} WHERE Dataset = @dataset AND Kind = 1 AND Version = @version",
            Parameters(("version", SpannerDbType.Int64, (long)snapshotVersion)));
        var length = await command.ExecuteScalarAsync();
        return length is long value ? value : throw NotFound(SnapshotKind, snapshotVersion);
    }

    /// <summary>Deletes the snapshots before <paramref name="snapshotVersion"/> and, unless kept, the deltas up to it.</summary>
    public async Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas)
    {
        var where = "Dataset = @dataset AND ((Kind = 1 AND Version < @version)"
                    + (keepAllDeltas ? ")" : " OR (Kind = 0 AND Version <= @version))");
        await using var connection = await Open();
        // ponytail: one transaction; split it if a cleanup ever nears the 80k mutations limit (tens of thousands of versions)
        await connection.RunWithRetriableTransactionAsync(async transaction =>
        {
            var blobIds = new List<string>();
            await using (var select = connection.CreateSelectCommand($"SELECT BlobId FROM {_versions} WHERE {where}",
                             Parameters(("version", SpannerDbType.Int64, (long)snapshotVersion))))
            {
                select.Transaction = transaction;
                await using var reader = await select.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    blobIds.Add(reader.GetString(0));
                }
            }

            if (blobIds.Count == 0)
            {
                return;
            }

            // The version rows first: once they are gone nothing points at the chunks
            await using var deleteVersions = connection.CreateDmlCommand($"DELETE FROM {_versions} WHERE {where}",
                Parameters(("version", SpannerDbType.Int64, (long)snapshotVersion)));
            deleteVersions.Transaction = transaction;
            await deleteVersions.ExecuteNonQueryAsync();
            await DeleteChunks(connection, transaction, blobIds);
        });
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        await Ready();
        await using var connection = await Open();
        await using var command = connection.CreateInsertOrUpdateCommand(_processed,
            Parameters(("Who", SpannerDbType.String, who), ("Version", SpannerDbType.Int64, (long)version)));
        await command.ExecuteNonQueryAsync();
    }

    public async Task<ulong> GetProcessedVersion(string who)
    {
        await Ready();
        await using var connection = await Open();
        await using var command = connection.CreateSelectCommand(
            $"SELECT Version FROM {_processed} WHERE Dataset = @dataset AND Who = @who",
            Parameters(("who", SpannerDbType.String, who)));
        return await command.ExecuteScalarAsync() is long version ? (ulong)version : 0;
    }

    public async Task<ulong> GetLowestProcessedVersion()
    {
        await Ready();
        await using var connection = await Open();
        await using var command = connection.CreateSelectCommand(
            $"SELECT MIN(Version) FROM {_processed} WHERE Dataset = @dataset", Parameters());
        return await command.ExecuteScalarAsync() is long version ? (ulong)version : 0;
    }

    private async Task<bool> Store(long kind, ulong version, byte[] index, Stream data)
    {
        await Ready();
        var blobId = Guid.NewGuid().ToString("N");
        await using var connection = await Open();
        var batch = new List<(long Part, long ChunkNo, byte[] Bytes)>();
        long dataLength = 0;

        // A blob over one commit goes in several; only the last one, with the version row, makes it visible
        foreach (var chunk in Chunks(IndexPart, new MemoryStream(index)).Concat(Chunks(DataPart, data)))
        {
            if (batch.Count == ChunksPerCommit)
            {
                await Commit(connection, blobId, batch, null);
                batch.Clear();
            }

            batch.Add(chunk);
            dataLength += chunk.Part == DataPart ? chunk.Bytes.Length : 0;
        }

        bool stored;
        try
        {
            stored = await Commit(connection, blobId, batch, (kind, version, dataLength));
        }
        catch (SpannerException e) when (e.ErrorCode == ErrorCode.AlreadyExists)
        {
            stored = false;
        }

        if (!stored)
        {
            // ponytail: a crash between the commits of a large blob leaves its chunks behind, unreferenced
            await connection.RunWithRetriableTransactionAsync(transaction => DeleteChunks(connection, transaction, [blobId]));
        }

        return stored;
    }

    /// <returns>False when the version row can't be stored: its delta version doesn't follow the latest version.</returns>
    private Task<bool> Commit(SpannerConnection connection, string blobId, List<(long Part, long ChunkNo, byte[] Bytes)> chunks,
        (long Kind, ulong Version, long DataLength)? versionRow) =>
        connection.RunWithRetriableTransactionAsync(async transaction =>
        {
            if (versionRow is { Kind: DeltaKind } delta && await LatestVersion(connection, transaction) != delta.Version - 1)
            {
                // The primary key alone would let a writer that is far behind re-insert a version that cleaning removed
                return false;
            }

            foreach (var (part, chunkNo, bytes) in chunks)
            {
                await using var insertChunk = connection.CreateInsertCommand(_chunks, Parameters(
                    ("BlobId", SpannerDbType.String, blobId),
                    ("Part", SpannerDbType.Int64, part),
                    ("ChunkNo", SpannerDbType.Int64, chunkNo),
                    ("Data", SpannerDbType.Bytes, bytes)));
                insertChunk.Transaction = transaction;
                await insertChunk.ExecuteNonQueryAsync();
            }

            if (versionRow is { } row)
            {
                // Insert, not upsert: the commit fails with AlreadyExists when another writer took this version
                await using var insertVersion = connection.CreateInsertCommand(_versions, Parameters(
                    ("Kind", SpannerDbType.Int64, row.Kind),
                    ("Version", SpannerDbType.Int64, (long)row.Version),
                    ("BlobId", SpannerDbType.String, blobId),
                    ("DataLength", SpannerDbType.Int64, row.DataLength),
                    ("CreatedAt", SpannerDbType.Timestamp, SpannerParameter.CommitTimestamp)));
                insertVersion.Transaction = transaction;
                await insertVersion.ExecuteNonQueryAsync();
            }

            return true;
        });

    /// <summary>The dataset's version: its latest delta, or its latest snapshot when cleaning removed the deltas up to it.</summary>
    private async Task<ulong> LatestVersion(SpannerConnection connection, SpannerTransaction transaction)
    {
        await using var command = connection.CreateSelectCommand(
            $"SELECT GREATEST(" +
            $"IFNULL((SELECT Version FROM {_versions} WHERE Dataset = @dataset AND Kind = 0 ORDER BY Version DESC LIMIT 1), 0), " +
            $"IFNULL((SELECT Version FROM {_versions} WHERE Dataset = @dataset AND Kind = 1 ORDER BY Version DESC LIMIT 1), 0))",
            Parameters());
        command.Transaction = transaction;
        return await command.ExecuteScalarAsync() is long version ? (ulong)version : 0;
    }

    private async Task DeleteChunks(SpannerConnection connection, SpannerTransaction transaction, List<string> blobIds)
    {
        await using var command = connection.CreateDmlCommand(
            $"DELETE FROM {_chunks} WHERE Dataset = @dataset AND BlobId IN UNNEST(@blobIds)",
            Parameters(("blobIds", SpannerDbType.ArrayOf(SpannerDbType.String), blobIds)));
        command.Transaction = transaction;
        await command.ExecuteNonQueryAsync();
    }

    private static IEnumerable<(long Part, long ChunkNo, byte[] Bytes)> Chunks(long part, Stream stream)
    {
        for (long chunkNo = 0; ; chunkNo++)
        {
            var size = stream.CanSeek ? (int)Math.Min(ChunkSize, stream.Length - stream.Position) : ChunkSize;
            var buffer = new byte[size];
            var read = size == 0 ? 0 : stream.ReadAtLeast(buffer, size, throwOnEndOfStream: false);
            if (read == 0)
            {
                yield break;
            }

            yield return (part, chunkNo, read == size ? buffer : buffer[..read]);
        }
    }

    private async Task<ReadOnlyMemory<byte>> ReadPart(long kind, ulong version, long part)
    {
        using var stream = new MemoryStream();
        await CopyPart(kind, version, part, stream);
        return stream.GetBuffer().AsMemory(0, (int)stream.Length);
    }

    private async Task CopyPart(long kind, ulong version, long part, Stream target)
    {
        await using var connection = await Open();
        // LEFT JOIN: a version with an empty part still returns a row, a missing version none
        await using var command = connection.CreateSelectCommand(
            $"SELECT c.Data FROM {_versions} v LEFT JOIN {_chunks} c " +
            "ON c.Dataset = v.Dataset AND c.BlobId = v.BlobId AND c.Part = @part " +
            "WHERE v.Dataset = @dataset AND v.Kind = @kind AND v.Version = @version ORDER BY c.ChunkNo",
            Parameters(("kind", SpannerDbType.Int64, kind), ("version", SpannerDbType.Int64, (long)version),
                ("part", SpannerDbType.Int64, part)));
        await using var reader = await command.ExecuteReaderAsync();
        var found = false;
        while (await reader.ReadAsync())
        {
            found = true;
            if (!reader.IsDBNull(0))
            {
                await target.WriteAsync(reader.GetFieldValue<byte[]>(0));
            }
        }

        if (!found)
        {
            throw NotFound(kind, version);
        }
    }

    /// <returns>Null when the version is missing or its data is shorter than the range.</returns>
    private async Task<byte[]?> ReadRange(long kind, ulong version, long start, int length)
    {
        await using var connection = await Open();
        // Each chunk returns just its piece of [@start, @end); SUBSTR counts from 1
        await using var command = connection.CreateSelectCommand(
            "SELECT SUBSTR(c.Data, GREATEST(@start - c.ChunkNo * @chunkSize, 0) + 1, " +
            "LEAST(@end, (c.ChunkNo + 1) * @chunkSize) - GREATEST(@start, c.ChunkNo * @chunkSize)) " +
            $"FROM {_versions} v JOIN {_chunks} c ON c.Dataset = v.Dataset AND c.BlobId = v.BlobId " +
            "WHERE v.Dataset = @dataset AND v.Kind = @kind AND v.Version = @version AND c.Part = 1 " +
            "AND c.ChunkNo BETWEEN DIV(@start, @chunkSize) AND DIV(@end - 1, @chunkSize) ORDER BY c.ChunkNo",
            Parameters(("kind", SpannerDbType.Int64, kind), ("version", SpannerDbType.Int64, (long)version),
                ("start", SpannerDbType.Int64, start), ("end", SpannerDbType.Int64, start + length),
                ("chunkSize", SpannerDbType.Int64, (long)ChunkSize)));
        await using var reader = await command.ExecuteReaderAsync();
        var result = new byte[length];
        var offset = 0;
        while (await reader.ReadAsync())
        {
            var piece = reader.GetFieldValue<byte[]>(0);
            piece.CopyTo(result, offset);
            offset += piece.Length;
        }

        return offset == length ? result : null;
    }

    private FileNotFoundException NotFound(long kind, ulong version) =>
        new($"{(kind == SnapshotKind ? "Snapshot" : "Delta")} {version} is not in {_versions}", _dataset);

    private SpannerParameterCollection Parameters(params (string Name, SpannerDbType Type, object Value)[] parameters)
    {
        var collection = new SpannerParameterCollection { { "Dataset", SpannerDbType.String, _dataset } };
        foreach (var (name, type, value) in parameters)
        {
            collection.Add(name, type, value);
        }

        return collection;
    }

    private async Task<SpannerConnection> Open()
    {
        var connection = new SpannerConnection(_connectionString);
        await connection.OpenAsync();
        return connection;
    }

    private Task Ready()
    {
        var schema = _schema;
        if (schema is null || schema.IsFaulted)
        {
            _schema = schema = EnsureSchema();
        }

        return schema;
    }

    private async Task EnsureSchema()
    {
        await using var connection = await Open();
        await using var count = connection.CreateSelectCommand(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@names)",
            new SpannerParameterCollection
            {
                { "names", SpannerDbType.ArrayOf(SpannerDbType.String), new[] { _versions, _chunks, _processed } }
            });
        if (await count.ExecuteScalarAsync<long>() == 3)
        {
            return;
        }

        // ponytail: no migrations; a different schema gets a different TablePrefix
        await using var ddl = connection.CreateDdlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {_versions} (
               Dataset STRING(MAX) NOT NULL,
               Kind INT64 NOT NULL,
               Version INT64 NOT NULL,
               BlobId STRING(32) NOT NULL,
               DataLength INT64 NOT NULL,
               CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
             ) PRIMARY KEY (Dataset, Kind, Version)
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {_chunks} (
               Dataset STRING(MAX) NOT NULL,
               BlobId STRING(32) NOT NULL,
               Part INT64 NOT NULL,
               ChunkNo INT64 NOT NULL,
               Data BYTES(MAX) NOT NULL,
             ) PRIMARY KEY (Dataset, BlobId, Part, ChunkNo)
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {_processed} (
               Dataset STRING(MAX) NOT NULL,
               Who STRING(MAX) NOT NULL,
               Version INT64 NOT NULL,
             ) PRIMARY KEY (Dataset, Who)
             """);
        await ddl.ExecuteNonQueryAsync();
    }
}
