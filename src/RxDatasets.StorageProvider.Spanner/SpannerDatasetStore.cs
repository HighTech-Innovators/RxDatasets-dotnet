using System.IO.Hashing;
using System.Text;
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

    private readonly SpannerConfig _config;
    private readonly string _dataset;
    private readonly string _versions;
    private readonly string _chunks;
    private readonly string _processed;
    private readonly string _leases;
    private Task? _schema;

    public SpannerDatasetStore(IOptions<SpannerConfig> config, IOptions<StorageConfig> storageConfig)
    {
        _config = config.Value;
        _dataset = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);
        var prefix = _config.TablePrefix;
        if (_config.TablePerDataset)
        {
            prefix += "_" + TableNamePart(_dataset);
        }

        _versions = prefix + "_Versions";
        _chunks = prefix + "_Chunks";
        _processed = prefix + "_ProcessedVersions";
        _leases = prefix + "_Leases";
        if (_processed.Length > 128)
        {
            throw new ArgumentException(
                $"Spanner table name {_processed} is over 128 characters; shorten the TablePrefix or the storage folder");
        }
    }

    /// <summary>The storage folder as a table name part; a hash of it when sanitizing changed it, so names stay unique.</summary>
    private static string TableNamePart(string folder)
    {
        var trimmed = folder.TrimEnd('/');
        var sanitized = Regex.Replace(trimmed, "[^A-Za-z0-9]+", "_").Trim('_');
        return sanitized == trimmed
            ? sanitized
            : $"{sanitized}_{Crc32.HashToUInt32(Encoding.UTF8.GetBytes(trimmed)):x8}";
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
        var latestDelta = reader.IsDBNull(1) ? 0 : (ulong)reader.GetInt64(1);
        var newest = Math.Max(latestDelta, snapshots.Count > 0 ? snapshots[^1].Version : 0);
        var oldest = !reader.IsDBNull(0) ? (ulong)reader.GetInt64(0) : newest == 0 ? 0 : newest + 1;
        return (oldest, newest, snapshots);
    }

    /// <returns>False when the version doesn't follow the latest one: another writer stored it first.</returns>
    public Task<bool> StoreDelta(ulong version, byte[] index, Stream data) => Store(DeltaKind, version, index, data);

    /// <returns>False when the version was already stored, by another writer.</returns>
    public Task<bool> StoreSnapshot(ulong version, byte[] index, Stream data) => Store(SnapshotKind, version, index, data);

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version) => ReadPart(DeltaKind, version, IndexPart);

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version) => ReadPart(SnapshotKind, version, IndexPart);

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion) =>
        ReadPart(SnapshotKind, snapshotVersion, DataPart);

    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback) =>
        ReadSnapshotDataAsync(snapshotVersion, stream =>
        {
            callback(stream);
            return Task.CompletedTask;
        });

    public async Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        // A temporary file, not a pipe: the callback may read synchronously, stop early or take its time, and a
        // snapshot can be larger than memory
        await using var file = new FileStream(DatasetHelper.GetTempFilePath(), FileMode.CreateNew, FileAccess.ReadWrite,
            FileShare.None, 81920, FileOptions.DeleteOnClose | FileOptions.Asynchronous);
        await CopyPart(SnapshotKind, snapshotVersion, DataPart, file);
        file.Position = 0;
        await asyncCallback(file);
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
            var kind = type == ObjectNameTypeEnum.Snapshot ? SnapshotKind : DeltaKind;
            var bytes = await ReadRange(kind, version, position, length);
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

    /// <summary>
    /// Deletes the snapshots before <paramref name="snapshotVersion"/> and, unless kept, the deltas up to it. Also
    /// removes chunks older than a day that no version points at: left behind by a writer that crashed while storing.
    /// </summary>
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

            // The version rows first: once they are gone nothing points at the chunks
            await using var deleteVersions = connection.CreateDmlCommand($"DELETE FROM {_versions} WHERE {where}",
                Parameters(("version", SpannerDbType.Int64, (long)snapshotVersion)));
            deleteVersions.Transaction = transaction;
            await deleteVersions.ExecuteNonQueryAsync();
            await DeleteChunks(connection, transaction, blobIds);

            // ponytail: scans every chunk row of the dataset on each cleanup; give Chunks a CreatedAt index if that
            // ever shows up in the cleaning time
            await using var deleteOrphans = connection.CreateDmlCommand(
                $"DELETE FROM {_chunks} c WHERE c.Dataset = @dataset " +
                "AND c.CreatedAt < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) " +
                $"AND NOT EXISTS (SELECT 1 FROM {_versions} v WHERE v.Dataset = c.Dataset AND v.BlobId = c.BlobId)",
                Parameters());
            deleteOrphans.Transaction = transaction;
            await deleteOrphans.ExecuteNonQueryAsync();
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

    /// <summary>
    /// Takes or renews the dataset's lease for <paramref name="owner"/>, by Spanner's clock: succeeds when nobody holds
    /// it, the holder's lease expired, or <paramref name="owner"/> already holds it.
    /// </summary>
    public async Task<bool> TryLease(string owner, TimeSpan duration)
    {
        await Ready();
        await using var connection = await Open();
        return await connection.RunWithRetriableTransactionAsync(async transaction =>
        {
            DateTime now;
            await using (var select = connection.CreateSelectCommand(
                             $"SELECT CURRENT_TIMESTAMP(), (SELECT Owner FROM {_leases} WHERE Dataset = @dataset), " +
                             $"(SELECT ExpiresAt FROM {_leases} WHERE Dataset = @dataset)", Parameters()))
            {
                select.Transaction = transaction;
                await using var reader = await select.ExecuteReaderAsync();
                await reader.ReadAsync();
                now = reader.GetFieldValue<DateTime>(0);
                if (!reader.IsDBNull(1) && reader.GetString(1) != owner && reader.GetFieldValue<DateTime>(2) > now)
                {
                    return false;
                }
            }

            await using var upsert = connection.CreateInsertOrUpdateCommand(_leases, Parameters(
                ("Owner", SpannerDbType.String, owner),
                ("ExpiresAt", SpannerDbType.Timestamp, now + duration)));
            upsert.Transaction = transaction;
            await upsert.ExecuteNonQueryAsync();
            return true;
        });
    }

    public async Task ReleaseLease(string owner)
    {
        await using var connection = await Open();
        await using var command = connection.CreateDmlCommand(
            $"DELETE FROM {_leases} WHERE Dataset = @dataset AND Owner = @owner",
            Parameters(("owner", SpannerDbType.String, owner)));
        await command.ExecuteNonQueryAsync();
    }

    private async Task<bool> Store(long kind, ulong version, byte[] index, Stream data)
    {
        await Ready();
        var blobId = Guid.NewGuid().ToString("N");
        await using var connection = await Open();
        bool stored;
        try
        {
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

            stored = await Commit(connection, blobId, batch, (kind, version, dataLength));
        }
        catch (SpannerException e) when (e.ErrorCode == ErrorCode.AlreadyExists)
        {
            // Another writer inserted the version row first
            stored = false;
        }

        // Only when the version row is known not to be stored: after another error the commit may still have
        // happened. Chunks left behind that way, or by a crash, DeleteOlderThan removes after a day.
        if (!stored)
        {
            await connection.RunWithRetriableTransactionAsync(transaction => DeleteChunks(connection, transaction, [blobId]));
        }

        return stored;
    }

    /// <returns>False when the version row can't be stored: its delta version doesn't follow the latest version.</returns>
    private Task<bool> Commit(SpannerConnection connection, string blobId,
        List<(long Part, long ChunkNo, byte[] Bytes)> chunks, (long Kind, ulong Version, long DataLength)? versionRow) =>
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
                    ("Data", SpannerDbType.Bytes, bytes),
                    ("CreatedAt", SpannerDbType.Timestamp, SpannerParameter.CommitTimestamp)));
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

    /// <summary>The dataset's version: its latest delta, or its latest snapshot once cleaning removed the deltas.</summary>
    private async Task<ulong> LatestVersion(SpannerConnection connection, SpannerTransaction transaction)
    {
        const string latest = "WHERE Dataset = @dataset AND Kind = {0} ORDER BY Version DESC LIMIT 1";
        await using var command = connection.CreateSelectCommand(
            $"SELECT GREATEST(IFNULL((SELECT Version FROM {_versions} {string.Format(latest, DeltaKind)}), 0), " +
            $"IFNULL((SELECT Version FROM {_versions} {string.Format(latest, SnapshotKind)}), 0))",
            Parameters());
        command.Transaction = transaction;
        return await command.ExecuteScalarAsync() is long version ? (ulong)version : 0;
    }

    private async Task DeleteChunks(SpannerConnection connection, SpannerTransaction transaction, List<string> blobIds)
    {
        if (blobIds.Count == 0)
        {
            return;
        }

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
        // One snapshot of the database: a version deleted between the two reads is missing, not an empty part
        await using var transaction = await connection.BeginReadOnlyTransactionAsync();
        var blobId = await BlobId(connection, kind, version, transaction) ?? throw NotFound(kind, version);
        // An empty part has no chunks
        await using var command = connection.CreateSelectCommand(
            $"SELECT Data FROM {_chunks} WHERE Dataset = @dataset AND BlobId = @blobId AND Part = @part ORDER BY ChunkNo",
            Parameters(("blobId", SpannerDbType.String, blobId), ("part", SpannerDbType.Int64, part)));
        command.Transaction = transaction;
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            await target.WriteAsync(reader.GetFieldValue<byte[]>(0));
        }
    }

    /// <summary>
    /// The blob of a version. A separate lookup rather than a join, so the chunk reads are plain key range scans
    /// (the emulator, at least, otherwise reads every chunk of the blob into memory).
    /// </summary>
    private async Task<string?> BlobId(SpannerConnection connection, long kind, ulong version,
        SpannerTransaction? transaction = null)
    {
        await using var command = connection.CreateSelectCommand(
            $"SELECT BlobId FROM {_versions} WHERE Dataset = @dataset AND Kind = @kind AND Version = @version",
            Parameters(("kind", SpannerDbType.Int64, kind), ("version", SpannerDbType.Int64, (long)version)));
        command.Transaction = transaction;
        return await command.ExecuteScalarAsync() as string;
    }

    /// <returns>Null when the version is missing or its data is shorter than the range.</returns>
    private async Task<byte[]?> ReadRange(long kind, ulong version, long start, int length)
    {
        await using var connection = await Open();
        // No read-only transaction needed: a version deleted after this lookup has no chunks left, so it comes out short
        // and counts as missing
        if (await BlobId(connection, kind, version) is not { } blobId)
        {
            return null;
        }

        // Each chunk returns just its piece of [@start, @end); SUBSTR counts from 1
        await using var command = connection.CreateSelectCommand(
            "SELECT SUBSTR(Data, GREATEST(@start - ChunkNo * @chunkSize, 0) + 1, " +
            "LEAST(@end, (ChunkNo + 1) * @chunkSize) - GREATEST(@start, ChunkNo * @chunkSize)) " +
            $"FROM {_chunks} WHERE Dataset = @dataset AND BlobId = @blobId AND Part = 1 " +
            "AND ChunkNo BETWEEN DIV(@start, @chunkSize) AND DIV(@end - 1, @chunkSize) ORDER BY ChunkNo",
            Parameters(("blobId", SpannerDbType.String, blobId),
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
        var connection = new SpannerConnection(_config.ConnectionString);
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
        string[] tables = [_versions, _chunks, _processed, _leases];
        await using var connection = await Open();
        await using var count = connection.CreateSelectCommand(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@names)",
            new SpannerParameterCollection { { "names", SpannerDbType.ArrayOf(SpannerDbType.String), tables } });
        if (await count.ExecuteScalarAsync<long>() == tables.Length)
        {
            return;
        }

        // ponytail: no migrations; a different schema gets a different TablePrefix
        string[] ddl =
        [
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
               CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
             ) PRIMARY KEY (Dataset, BlobId, Part, ChunkNo)
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {_processed} (
               Dataset STRING(MAX) NOT NULL,
               Who STRING(MAX) NOT NULL,
               Version INT64 NOT NULL,
             ) PRIMARY KEY (Dataset, Who)
             """,
            $"""
             CREATE TABLE IF NOT EXISTS {_leases} (
               Dataset STRING(MAX) NOT NULL,
               Owner STRING(MAX) NOT NULL,
               ExpiresAt TIMESTAMP NOT NULL,
             ) PRIMARY KEY (Dataset)
             """
        ];
        if (!_config.CreateTables)
        {
            throw new InvalidOperationException(
                $"Spanner tables for {_dataset} are missing. Create them, or set Spanner:CreateTables to let this host "
                + $"do it (needs spanner.databases.updateDdl):{Environment.NewLine}"
                + string.Join(";" + Environment.NewLine, ddl));
        }

        await using var command = connection.CreateDdlCommand(ddl[0], ddl[1..]);
        await command.ExecuteNonQueryAsync();
    }
}
