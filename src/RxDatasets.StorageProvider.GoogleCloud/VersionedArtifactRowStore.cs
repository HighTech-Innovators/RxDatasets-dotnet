using System.Globalization;
using System.Text.RegularExpressions;
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
    Task<(string Token, DateTimeOffset Expiry)?> TryAcquireLock(string datasetPrefix, string lockName, TimeSpan duration, CancellationToken cancellationToken = default);
    Task<bool> RenewLock(string datasetPrefix, string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken = default);
    Task ReleaseLock(string datasetPrefix, string lockName, string lockToken, CancellationToken cancellationToken = default);
    IAsyncEnumerable<HybridArtifactRecord> GetArtifactRange(string datasetPrefix, ObjectNameTypeEnum type, ulong fromVersionExclusive, ulong toVersionInclusive, CancellationToken cancellationToken = default);
    Task RequestLock(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default);
    Task<bool> ShouldYield(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default);
    Task DeleteRequestLock(string datasetPrefix, string requestLockName, CancellationToken cancellationToken = default);
}

public sealed class GoogleCloudVersionedArtifactRowStore : IVersionedArtifactRowStore
{
    private const string MetadataFamily = "meta";
    private const string PayloadFamily = "payload";
    private const string FallbackFamily = "fallback";
    private const string ProcessedFamily = "processed";
    private const string LockFamily = "lock";
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

    public async IAsyncEnumerable<HybridArtifactRecord> GetArtifactRange(
        string datasetPrefix,
        ObjectNameTypeEnum type,
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();

        var startKey = GetArtifactRowKey(datasetPrefix, type, fromVersionExclusive + 1);
        // +1 because EndKeyClosed is inclusive and version is zero-padded D20
        var endKey = GetArtifactRowKey(datasetPrefix, type, toVersionInclusive);

        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet()
        };
        request.Rows.RowRanges.Add(new RowRange
        {
            StartKeyClosed = ByteString.CopyFromUtf8(startKey),
            EndKeyClosed = ByteString.CopyFromUtf8(endKey)
        });

        var rows = _bigtableClient.ReadRows(request);
        await foreach (var row in rows.WithCancellation(cancellationToken))
        {
            yield return ParseArtifactRow(row);
        }
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

    public async Task<(string Token, DateTimeOffset Expiry)?> TryAcquireLock(string datasetPrefix, string lockName, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, lockName);
        var token = Guid.NewGuid().ToString();
        var expiry = DateTimeOffset.UtcNow.Add(duration);
        var expiryBytes = ByteString.CopyFromUtf8(expiry.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        var tokenBytes = ByteString.CopyFromUtf8(token);

        // Check if lock is free or expired
        var condition = new RowFilter
        {
            Condition = new RowFilter.Types.Condition
            {
                PredicateFilter = new RowFilter
                {
                    Chain = new RowFilter.Types.Chain
                    {
                        Filters =
                        {
                            new RowFilter { FamilyNameRegexFilter = LockFamily },
                            new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("expiry") },
                            new RowFilter { ValueRangeFilter = new ValueRange { EndValueOpen = ByteString.CopyFromUtf8(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)) } }
                        }
                    }
                },
                TrueFilter = new RowFilter
                {
                    Chain = new RowFilter.Types.Chain
                    {
                        Filters = { new RowFilter { StripValueTransformer = true } } // Dummy filter, we just want mutations
                    }
                }
            }
        };

        // Simplified approach: Read-Modify-Write using CheckAndMutate
        // Ideally we want: IF (token not exists OR expiry < now) THEN set token, set expiry
        
        // Since Bigtable CheckAndMutate is limited, we can use a conditional mutation based on timestamp or value.
        // But comparing value (timestamp string) lexicographically works for string timestamps if fixed width... which unix millis are not always.
        // Let's rely on the fact that we can read first. But that's not atomic.
        
        // Better atomic approach:
        // We can't easily do numeric comparison in CheckAndMutate.
        // However, we can use the "Timestamp" of the cell itself if we store expiry as the cell timestamp?
        // No, Bigtable cell timestamps are for versioning.
        
        // Alternate atomic approach:
        // 1. Read row.
        // 2. If locked and valid, return null.
        // 3. If unlocked or expired, try CheckAndMutate(expecting the value we just read).
        
        // Let's implement Optimistic Concurrency Control (OCC).
        
        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet { RowKeys = { ByteString.CopyFromUtf8(rowKey) } }
        };
        
        string? currentToken = null;
        long currentExpiry = 0;
        
        await using (var enumerator = _bigtableClient.ReadRows(request).GetAsyncEnumerator(cancellationToken))
        {
            if (await enumerator.MoveNextAsync())
            {
                var row = enumerator.Current;
                currentToken = TryReadString(row, LockFamily, "token");
                var expiryStr = TryReadString(row, LockFamily, "expiry");
                if (long.TryParse(expiryStr, out var e)) currentExpiry = e;
            }
        }

        if (currentToken != null && currentExpiry > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())
        {
            return null; // Locked by someone else
        }

        // It is free or expired. Try to grab it.
        // We condition on the "token" being what we saw (or absent).
        
        var matchFilter = currentToken == null 
            ? new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") } } } } // Fails if any token exists? No, this just filters.
            : new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") }, new RowFilter { ValueRegexFilter = ByteString.CopyFromUtf8(Regex.Escape(currentToken)) } } } };

        // If currentToken is null, we want to match if "token" column does NOT exist.
        // Bigtable CheckAndMutate: "If the filter returns any cells, then true_mutations are executed"
        
        // If currentToken is NULL (no lock):
        // Condition: Row has "token" column?
        // If YES: Fail (someone created it).
        // If NO: Success (acquire).
        
        // If currentToken is SET (but expired):
        // Condition: Token value == currentToken?
        // If YES: Success (overwrite).
        // If NO: Fail (someone else grabbed it).

        Mutation mutation = new Mutation
        {
            SetCell = new Mutation.Types.SetCell { FamilyName = LockFamily, ColumnQualifier = ByteString.CopyFromUtf8("token"), Value = tokenBytes }
        };
        Mutation mutationExpiry = new Mutation
        {
            SetCell = new Mutation.Types.SetCell { FamilyName = LockFamily, ColumnQualifier = ByteString.CopyFromUtf8("expiry"), Value = expiryBytes }
        };

        CheckAndMutateRowRequest checkRequest;
        
        if (currentToken == null)
        {
            checkRequest = new CheckAndMutateRowRequest
            {
                TableNameAsTableName = GetTableName(),
                RowKey = ByteString.CopyFromUtf8(rowKey),
                PredicateFilter = new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") }, new RowFilter { CellsPerColumnLimitFilter = 1 } } } },
                FalseMutations = { mutation, mutationExpiry } // If NO cells found (no token), execute these
            };
        }
        else
        {
            checkRequest = new CheckAndMutateRowRequest
            {
                TableNameAsTableName = GetTableName(),
                RowKey = ByteString.CopyFromUtf8(rowKey),
                PredicateFilter = new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") }, new RowFilter { ValueRegexFilter = ByteString.CopyFromUtf8("^" + Regex.Escape(currentToken) + "$") } } } },
                TrueMutations = { mutation, mutationExpiry } // If token MATCHES what we saw (expired one), execute these
            };
        }

        var response = await _bigtableClient.CheckAndMutateRowAsync(checkRequest);
        
        // If we expected NULL and predicate was false (no cells) -> FalseMutations executed -> Success
        if (currentToken == null && !response.PredicateMatched) return (token, expiry);
        
        // If we expected VALUE and predicate was true (matched) -> TrueMutations executed -> Success
        if (currentToken != null && response.PredicateMatched) return (token, expiry);

        return null;
    }

    public async Task<bool> RenewLock(string datasetPrefix, string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, lockName);
        var expiry = DateTimeOffset.UtcNow.Add(duration);
        var expiryBytes = ByteString.CopyFromUtf8(expiry.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));

        // Condition: Token == lockToken
        var checkRequest = new CheckAndMutateRowRequest
        {
            TableNameAsTableName = GetTableName(),
            RowKey = ByteString.CopyFromUtf8(rowKey),
            PredicateFilter = new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") }, new RowFilter { ValueRegexFilter = ByteString.CopyFromUtf8("^" + Regex.Escape(lockToken) + "$") } } } },
            TrueMutations = 
            { 
                new Mutation { SetCell = new Mutation.Types.SetCell { FamilyName = LockFamily, ColumnQualifier = ByteString.CopyFromUtf8("expiry"), Value = expiryBytes } }
            }
        };

        var response = await _bigtableClient.CheckAndMutateRowAsync(checkRequest);
        return response.PredicateMatched;
    }

    public async Task ReleaseLock(string datasetPrefix, string lockName, string lockToken, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, lockName);

        // Condition: Token == lockToken
        // Action: Delete row
        var checkRequest = new CheckAndMutateRowRequest
        {
            TableNameAsTableName = GetTableName(),
            RowKey = ByteString.CopyFromUtf8(rowKey),
            PredicateFilter = new RowFilter { Chain = new RowFilter.Types.Chain { Filters = { new RowFilter { FamilyNameRegexFilter = LockFamily }, new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("token") }, new RowFilter { ValueRegexFilter = ByteString.CopyFromUtf8("^" + Regex.Escape(lockToken) + "$") } } } },
            TrueMutations = 
            { 
                new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() }
            }
        };

        await _bigtableClient.CheckAndMutateRowAsync(checkRequest);
    }

    public async Task RequestLock(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, requestLockName);
        
        await _bigtableClient.MutateRowAsync(GetTableName(), ByteString.CopyFromUtf8(rowKey), new Mutation[]
        {
            new Mutation { SetCell = new Mutation.Types.SetCell { FamilyName = LockFamily, ColumnQualifier = ByteString.CopyFromUtf8("requester"), Value = ByteString.CopyFromUtf8(instanceId) } }
        });
    }

    public async Task<bool> ShouldYield(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, requestLockName);
        var request = new ReadRowsRequest
        {
            TableNameAsTableName = GetTableName(),
            Rows = new RowSet { RowKeys = { ByteString.CopyFromUtf8(rowKey) } }
        };

        await using (var enumerator = _bigtableClient.ReadRows(request).GetAsyncEnumerator(cancellationToken))
        {
            if (await enumerator.MoveNextAsync())
            {
                var requester = TryReadString(enumerator.Current, LockFamily, "requester");
                return requester != null && requester != instanceId;
            }
        }
        return false;
    }

    public async Task DeleteRequestLock(string datasetPrefix, string requestLockName, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var rowKey = GetLockRowKey(datasetPrefix, requestLockName);
        await _bigtableClient.MutateRowAsync(GetTableName(), ByteString.CopyFromUtf8(rowKey), new Mutation[]
        {
            new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() }
        });
    }

    private static string GetLockRowKey(string datasetPrefix, string lockName)
    {
        // lockName passed in is full object path "prefix/lock-file"
        // We want to store it in a way that doesn't conflict with artifacts.
        // We can just use "lock|{datasetPrefix}|{lockNameSuffix}"
        // But datasetPrefix includes the trailing slash.
        // Let's just use "lock|" + lockName (which already contains prefix)
        // But we need to be careful about uniqueness.
        // Let's use the provided lockName as unique identifier but prefixed with "lock|"
        return $"lock|{lockName}";
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
        Table? table = null;

        try
        {
            table = await _tableAdminClient.GetTableAsync(tableName.ToString());
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
        {
            // Table not found, will create below
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Table not found, will create below
        }

        if (table == null)
        {
            await CreateTable(tableName);
        }
        else
        {
            var modifications = new List<ModifyColumnFamiliesRequest.Types.Modification>();

            if (!table.ColumnFamilies.ContainsKey(LockFamily))
            {
                modifications.Add(new ModifyColumnFamiliesRequest.Types.Modification
                {
                    Id = LockFamily,
                    Create = new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } }
                });
            }

            if (modifications.Count > 0)
            {
                await _tableAdminClient.ModifyColumnFamiliesAsync(tableName, modifications);
            }
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
                    { ProcessedFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } },
                    { LockFamily, new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } }
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
