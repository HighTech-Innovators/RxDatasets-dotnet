using System.Globalization;
using Google.Api.Gax;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageProvider.GoogleCloud;

public readonly record struct ArtifactWritePlan(bool StoreIndexInline, bool StoreDataInline);

public sealed record HybridArtifactRecord(
    ObjectNameTypeEnum Type,
    ulong Version,
    DateTimeOffset CreatedAt,
    byte[]? InlineIndex,
    byte[]? InlineData,
    string? IndexFallbackObjectKey,
    string? DataFallbackObjectKey,
    long IndexLength,
    long DataLength);

public readonly record struct HybridArtifactSummary(ulong Version, DateTimeOffset CreatedAt);

public interface IVersionedArtifactRowStore
{
    ArtifactWritePlan CreateWritePlan(long indexLength, long dataLength);
    Task UpsertArtifact(string datasetPrefix, HybridArtifactRecord artifact, CancellationToken cancellationToken = default);
    Task<HybridArtifactRecord?> GetArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<HybridArtifactSummary>> ListArtifacts(string datasetPrefix, ObjectNameTypeEnum type, CancellationToken cancellationToken = default);
    Task DeleteArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<string, ulong>> GetProcessedVersions(string datasetPrefix, CancellationToken cancellationToken = default);
    Task PutProcessedVersion(string datasetPrefix, string who, ulong version, CancellationToken cancellationToken = default);
}

public sealed class GoogleCloudVersionedArtifactRowStore : IVersionedArtifactRowStore
{
    private const string MetadataFamily = "meta";
    private const string PayloadFamily = "payload";
    private const string FallbackFamily = "fallback";
    private const string ProcessedFamily = "processed";
    private const string QualifierKind = "kind";
    private const string QualifierVersion = "version";
    private const string QualifierCreatedAt = "created-at";
    private const string QualifierIndexLength = "index-length";
    private const string QualifierDataLength = "data-length";
    private const string QualifierIndex = "index";
    private const string QualifierData = "data";
    private const string QualifierIndexObject = "index-object";
    private const string QualifierDataObject = "data-object";

    private readonly BigtableClient _bigtableClient;
    private readonly BigtableTableAdminClient _tableAdminClient;
    private readonly string _bigtableTableId;
    private readonly string _bigtableProjectId;
    private readonly string _bigtableInstanceId;
    private readonly long _maxInlinePayloadBytes;
    private bool _initialized;

    public GoogleCloudVersionedArtifactRowStore(IOptions<GoogleCloudConfig> googleCloudConfig, bool retryInitialization = true)
    {
        _bigtableTableId = googleCloudConfig.Value.BigtableTableId ?? throw new ArgumentNullException(nameof(googleCloudConfig), "BigtableTableId must be set");
        _bigtableProjectId = googleCloudConfig.Value.BigtableProjectId ?? throw new ArgumentNullException(nameof(googleCloudConfig), "BigtableProjectId must be set");
        _bigtableInstanceId = googleCloudConfig.Value.BigtableInstanceId ?? throw new ArgumentNullException(nameof(googleCloudConfig), "BigtableInstanceId must be set");
        _maxInlinePayloadBytes = googleCloudConfig.Value.MaxInlinePayloadBytes;

        Google.Apis.Auth.OAuth2.GoogleCredential? credential = null;
        if (!string.IsNullOrEmpty(googleCloudConfig.Value.JsonCredentials))
        {
            credential = Google.Apis.Auth.OAuth2.GoogleCredential.FromJson(googleCloudConfig.Value.JsonCredentials);
        }

        var adminClientBuilder = new BigtableTableAdminClientBuilder();
        if (credential != null)
        {
            adminClientBuilder.Credential = credential;
        }

        var bigtableClientBuilder = new BigtableClientBuilder();
        if (credential != null)
        {
            bigtableClientBuilder.Credential = credential;
        }

        _tableAdminClient = adminClientBuilder.Build();
        _bigtableClient = bigtableClientBuilder.Build();

        if (retryInitialization)
        {
            _ = InitializationLoop();
        }
        else
        {
            Initialize().GetAwaiter().GetResult();
        }
    }

    public ArtifactWritePlan CreateWritePlan(long indexLength, long dataLength)
    {
        return new ArtifactWritePlan(indexLength <= _maxInlinePayloadBytes, dataLength <= _maxInlinePayloadBytes);
    }

    public async Task UpsertArtifact(string datasetPrefix, HybridArtifactRecord artifact, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var rowKey = GetArtifactRowKey(datasetPrefix, artifact.Type, artifact.Version);
        var entry = new MutateRowsRequest.Types.Entry
        {
            RowKey = ByteString.CopyFromUtf8(rowKey),
            Mutations =
            {
                new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() }
            }
        };

        AddStringCell(entry, MetadataFamily, QualifierKind, GetKindSegment(artifact.Type));
        AddStringCell(entry, MetadataFamily, QualifierVersion, artifact.Version.ToString(CultureInfo.InvariantCulture));
        AddStringCell(entry, MetadataFamily, QualifierCreatedAt, artifact.CreatedAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        AddStringCell(entry, MetadataFamily, QualifierIndexLength, artifact.IndexLength.ToString(CultureInfo.InvariantCulture));
        AddStringCell(entry, MetadataFamily, QualifierDataLength, artifact.DataLength.ToString(CultureInfo.InvariantCulture));

        if (artifact.InlineIndex != null)
        {
            AddBytesCell(entry, PayloadFamily, QualifierIndex, artifact.InlineIndex);
        }

        if (artifact.InlineData != null)
        {
            AddBytesCell(entry, PayloadFamily, QualifierData, artifact.InlineData);
        }

        if (!string.IsNullOrEmpty(artifact.IndexFallbackObjectKey))
        {
            AddStringCell(entry, FallbackFamily, QualifierIndexObject, artifact.IndexFallbackObjectKey);
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            AddStringCell(entry, FallbackFamily, QualifierDataObject, artifact.DataFallbackObjectKey);
        }

        await _bigtableClient.MutateRowsAsync(new MutateRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Entries = { entry }
        }, cancellationToken: cancellationToken);
    }

    public async Task<HybridArtifactRecord?> GetArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var rowKey = GetArtifactRowKey(datasetPrefix, type, version);
        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet()
        };
        request.Rows.RowKeys.Add(ByteString.CopyFromUtf8(rowKey));

        var rows = _bigtableClient.ReadRows(request);
        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            if (row.Key.ToStringUtf8() == rowKey)
            {
                return ParseArtifactRow(row);
            }
        }

        return null;
    }

    public async Task<IReadOnlyList<HybridArtifactSummary>> ListArtifacts(string datasetPrefix, ObjectNameTypeEnum type, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var prefix = GetArtifactRowPrefix(datasetPrefix, type);
        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet(),
            Filter = new RowFilter { FamilyNameRegexFilter = MetadataFamily }
        };
        request.Rows.RowRanges.Add(new RowRange
        {
            StartKeyClosed = ByteString.CopyFromUtf8(prefix),
            EndKeyOpen = ByteString.CopyFromUtf8(GetPrefixEnd(prefix))
        });

        var summaries = new List<HybridArtifactSummary>();
        var rows = _bigtableClient.ReadRows(request);
        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            var rowKey = row.Key.ToStringUtf8();
            if (!rowKey.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var createdAt = TryReadString(row, MetadataFamily, QualifierCreatedAt);
            var createdAtValue = createdAt != null
                ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(createdAt, CultureInfo.InvariantCulture))
                : DateTimeOffset.UtcNow;

            summaries.Add(new HybridArtifactSummary(ParseVersionFromRowKey(rowKey), createdAtValue));
        }

        return summaries;
    }

    public async Task DeleteArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var entry = new MutateRowsRequest.Types.Entry
        {
            RowKey = ByteString.CopyFromUtf8(GetArtifactRowKey(datasetPrefix, type, version)),
            Mutations =
            {
                new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() }
            }
        };

        await _bigtableClient.MutateRowsAsync(new MutateRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Entries = { entry }
        }, cancellationToken: cancellationToken);
    }

    public async Task<IReadOnlyDictionary<string, ulong>> GetProcessedVersions(string datasetPrefix, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var rowKey = GetProcessedVersionsRowKey(datasetPrefix);
        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet(),
            Filter = new RowFilter { FamilyNameRegexFilter = ProcessedFamily }
        };
        request.Rows.RowKeys.Add(ByteString.CopyFromUtf8(rowKey));

        var result = new Dictionary<string, ulong>(StringComparer.Ordinal);
        var rows = _bigtableClient.ReadRows(request);
        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            foreach (var family in row.Families.Where(f => f.Name == ProcessedFamily))
            {
                foreach (var column in family.Columns)
                {
                    if (column.Cells.Count == 0)
                    {
                        continue;
                    }

                    var who = column.Qualifier.ToStringUtf8();
                    var version = ulong.Parse(column.Cells[0].Value.ToStringUtf8(), CultureInfo.InvariantCulture);
                    result[who] = version;
                }
            }
        }

        return result;
    }

    public async Task PutProcessedVersion(string datasetPrefix, string who, ulong version, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var entry = new MutateRowsRequest.Types.Entry
        {
            RowKey = ByteString.CopyFromUtf8(GetProcessedVersionsRowKey(datasetPrefix))
        };
        AddStringCell(entry, ProcessedFamily, who, version.ToString(CultureInfo.InvariantCulture));

        await _bigtableClient.MutateRowsAsync(new MutateRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Entries = { entry }
        }, cancellationToken: cancellationToken);
    }

    private async Task EnsureInitialized()
    {
        if (_initialized)
        {
            return;
        }

        for (var i = 0; i < 1000; i++)
        {
            await Task.Delay(500);
            if (_initialized)
            {
                return;
            }
        }

        throw new InvalidOperationException("Bigtable row store not yet initialized");
    }

    private async Task Initialize()
    {
        var tableName = GetTableName();

        try
        {
            await _tableAdminClient.GetTableAsync(tableName.ToString());
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            await CreateTable(tableName);
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            await CreateTable(tableName);
        }

        _initialized = true;
    }

    private async Task InitializationLoop()
    {
        while (true)
        {
            try
            {
                await Initialize();
                break;
            }
            catch
            {
                await Task.Delay(5000);
            }
        }
    }

    private async Task CreateTable(TableName tableName)
    {
        await _tableAdminClient.CreateTableAsync(new CreateTableRequest
        {
            ParentAsInstanceName = new InstanceName(_bigtableProjectId, _bigtableInstanceId),
            TableId = _bigtableTableId,
            Table = new Table
            {
                ColumnFamilies =
                {
                    { MetadataFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } },
                    { PayloadFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } },
                    { FallbackFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } },
                    { ProcessedFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } }
                }
            }
        });
    }

    private TableName GetTableName()
    {
        return new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
    }

    private static void AddStringCell(MutateRowsRequest.Types.Entry entry, string family, string qualifier, string value)
    {
        AddBytesCell(entry, family, qualifier, System.Text.Encoding.UTF8.GetBytes(value));
    }

    private static void AddBytesCell(MutateRowsRequest.Types.Entry entry, string family, string qualifier, byte[] value)
    {
        entry.Mutations.Add(new Mutation
        {
            SetCell = new Mutation.Types.SetCell
            {
                FamilyName = family,
                ColumnQualifier = ByteString.CopyFromUtf8(qualifier),
                TimestampMicros = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000,
                Value = ByteString.CopyFrom(value)
            }
        });
    }

    private static HybridArtifactRecord ParseArtifactRow(Row row)
    {
        var kind = TryReadString(row, MetadataFamily, QualifierKind);
        var type = string.Equals(kind, FolderNames.SnapshotsFolderName, StringComparison.Ordinal)
            ? ObjectNameTypeEnum.Snapshot
            : ObjectNameTypeEnum.Delta;
        var version = ulong.Parse(TryReadString(row, MetadataFamily, QualifierVersion) ?? throw new InvalidOperationException("Artifact version is missing"), CultureInfo.InvariantCulture);
        var createdAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(TryReadString(row, MetadataFamily, QualifierCreatedAt) ?? "0", CultureInfo.InvariantCulture));
        var indexLength = long.Parse(TryReadString(row, MetadataFamily, QualifierIndexLength) ?? "0", CultureInfo.InvariantCulture);
        var dataLength = long.Parse(TryReadString(row, MetadataFamily, QualifierDataLength) ?? "0", CultureInfo.InvariantCulture);

        return new HybridArtifactRecord(
            type,
            version,
            createdAt,
            TryReadBytes(row, PayloadFamily, QualifierIndex),
            TryReadBytes(row, PayloadFamily, QualifierData),
            TryReadString(row, FallbackFamily, QualifierIndexObject),
            TryReadString(row, FallbackFamily, QualifierDataObject),
            indexLength,
            dataLength);
    }

    private static string? TryReadString(Row row, string familyName, string qualifier)
    {
        var bytes = TryReadBytes(row, familyName, qualifier);
        return bytes == null ? null : System.Text.Encoding.UTF8.GetString(bytes);
    }

    private static byte[]? TryReadBytes(Row row, string familyName, string qualifier)
    {
        foreach (var family in row.Families)
        {
            if (!string.Equals(family.Name, familyName, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var column in family.Columns)
            {
                if (!string.Equals(column.Qualifier.ToStringUtf8(), qualifier, StringComparison.Ordinal) || column.Cells.Count == 0)
                {
                    continue;
                }

                return column.Cells[0].Value.ToByteArray();
            }
        }

        return null;
    }

    private static string GetArtifactRowPrefix(string datasetPrefix, ObjectNameTypeEnum type)
    {
        return $"artifact|{datasetPrefix}|{GetKindSegment(type)}|";
    }

    private static string GetArtifactRowKey(string datasetPrefix, ObjectNameTypeEnum type, ulong version)
    {
        return $"{GetArtifactRowPrefix(datasetPrefix, type)}{version:D20}";
    }

    private static string GetProcessedVersionsRowKey(string datasetPrefix)
    {
        return $"processed|{datasetPrefix}";
    }

    private static string GetKindSegment(ObjectNameTypeEnum type)
    {
        return type switch
        {
            ObjectNameTypeEnum.Delta => FolderNames.DeltasFolderName,
            ObjectNameTypeEnum.Snapshot => FolderNames.SnapshotsFolderName,
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
    }

    private static ulong ParseVersionFromRowKey(string rowKey)
    {
        var versionSegment = rowKey[(rowKey.LastIndexOf('|') + 1)..];
        return ulong.Parse(versionSegment, CultureInfo.InvariantCulture);
    }

    private static string GetPrefixEnd(string prefix)
    {
        return prefix + '\uffff';
    }

}
