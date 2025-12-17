using System.IO.Pipelines;
using System.Net.Http.Headers;
using Google.Cloud.Bigtable.V2;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.Admin.V2;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Storage.V1;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    public class HybridBigtableGcsStorage<T> : IDiskStorage<T>, IDisposable
    {
        private readonly StorageClient _storageClient;
        private readonly BigtableClient _bigtableClient;
        private readonly BigtableTableAdminClient _tableAdminClient;
        private readonly ILogger<HybridBigtableGcsStorage<T>> _logger;
        private readonly string _bucketName;
        private readonly string _bigtableTableId;
        private readonly string _bigtableProjectId;
        private readonly string _bigtableInstanceId;
        private readonly long _sizeThresholdBytes;
        private bool _initialized;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public HybridBigtableGcsStorage(ILogger<HybridBigtableGcsStorage<T>> logger, IOptions<GoogleCloudConfig> googleCloudConfig, bool retryInitialization = true)
        {
            _logger = logger;
            _bucketName = googleCloudConfig.Value.BucketName;
            _bigtableTableId = googleCloudConfig.Value.BigtableTableId ?? throw new ArgumentNullException("BigtableTableId must be set");
            _bigtableProjectId = googleCloudConfig.Value.BigtableProjectId ?? throw new ArgumentNullException("BigtableProjectId must be set");
            _bigtableInstanceId = googleCloudConfig.Value.BigtableInstanceId ?? throw new ArgumentNullException("BigtableInstanceId must be set");
            _sizeThresholdBytes = googleCloudConfig.Value.SizeThresholdBytes;

            var clientBuilder = new StorageClientBuilder();

            // Check if we're running against an emulator
            var emulatorHost = Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
            if (!string.IsNullOrEmpty(emulatorHost))
            {
                // Configure for emulator use
                clientBuilder.BaseUri = $"http://{emulatorHost}/storage/v1/";
                clientBuilder.UnauthenticatedAccess = true;
            }
            else if (!string.IsNullOrEmpty(googleCloudConfig.Value.JsonCredentials))
            {
                clientBuilder.JsonCredentials = googleCloudConfig.Value.JsonCredentials;
            }

            _storageClient = clientBuilder.Build();

            // Bigtable clients
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
            _tableAdminClient = adminClientBuilder.Build();

            var bigtableClientBuilder = new BigtableClientBuilder();
            if (credential != null)
            {
                bigtableClientBuilder.Credential = credential;
            }
            _bigtableClient = bigtableClientBuilder.Build();

            _ = retryInitialization ? InitializationLoop() : Initialize();
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        private async Task EnsureInitialized()
        {
            if (_initialized)
                return;

            for (var i = 0; i < 1000; i++)
            {
                await Task.Delay(500);
                if (_initialized)
                    return;
            }
            throw new InvalidOperationException("Hybrid storage not yet initialized");
        }

        private async Task Initialize()
        {
            try
            {
                await EnsureBucketExistsAsync();
                await EnsureBigtableTableExistsAsync();
                _initialized = true;
                _logger.LogInformation("Hybrid Bigtable+GCS storage access verified");
                SetHealthy();
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError(ex, "Could not init Hybrid storage");
                throw;
            }
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
                catch (Exception ex)
                {
                    SetUnhealthy(ex);
                    _logger.LogError(ex, "Failed to initialize hybrid storage provider, retrying...");
                    await Task.Delay(5000);
                }
            }
        }

        private async Task EnsureBucketExistsAsync()
        {
            try
            {
                await _storageClient.GetBucketAsync(_bucketName);
                SetHealthy();
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                try
                {
                    var projectId = Environment.GetEnvironmentVariable("GOOGLE_PROJECT_ID") ?? throw new InvalidOperationException("GOOGLE_PROJECT_ID not set");
                    await _storageClient.CreateBucketAsync(projectId, new Google.Apis.Storage.v1.Data.Bucket { Name = _bucketName, Location = "eu-north-1" });
                    SetHealthy();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to create GCS bucket");
                    SetUnhealthy(ex);
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure GCS bucket exists");
                SetUnhealthy(ex);
                throw;
            }
        }

        private async Task EnsureBigtableTableExistsAsync()
        {
            try
            {
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var table = await _tableAdminClient.GetTableAsync(tableName.ToString());
                if (table == null || table.ColumnFamilies.Count == 0)
                {
                    // Create table with column family if not exists
                    var createTableRequest = new CreateTableRequest
                    {
                        ParentAsInstanceName = new InstanceName(_bigtableProjectId, _bigtableInstanceId),
                        TableId = _bigtableTableId,
                        Table = new Table
                        {
                            ColumnFamilies =
                            {
                                { "data", new ColumnFamily { GcRule = new GcRule { MaxNumVersions = 1 } } }
                            }
                        }
                    };
                    await _tableAdminClient.CreateTableAsync(createTableRequest);
                    _logger.LogInformation("Created Bigtable table {TableId}", _bigtableTableId);
                }
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure Bigtable table exists");
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task DeleteFile(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                // Try delete from Bigtable
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var mutateRowsRequest = new MutateRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Entries =
                    {
                        new MutateRowsRequest.Types.Entry
                        {
                            RowKey = ByteString.CopyFromUtf8(objectKey),
                            Mutations = { new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() } }
                        }
                    }
                };
                await _bigtableClient.MutateRowsAsync(mutateRowsRequest);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete from Bigtable for {objectKey}", objectKey);
            }

            try
            {
                // Delete from GCS
                await _storageClient.DeleteObjectAsync(_bucketName, objectKey);
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // OK if not found
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete from GCS for {objectKey}", objectKey);
                SetUnhealthy(ex);
                throw new InvalidOperationException("Hybrid storage unavailable", ex);
            }
            SetHealthy();
        }

        public async Task DeleteFolder(string prefix)
        {
            await EnsureInitialized();
            _logger.LogInformation("Deleting 'folder' {folderName}", prefix);
            try
            {
                // Normalize prefix
                var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';

                // Delete from Bigtable
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var readRowsRequest = new ReadRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Rows = new RowSet(),
                    RowsLimit = 1000
                };
                readRowsRequest.Rows.RowRanges.Add(new RowRange { StartKeyClosed = ByteString.CopyFromUtf8(normalizedPrefix) });
                var stream = _bigtableClient.ReadRows(readRowsRequest);
                var mutateRequests = new List<MutateRowsRequest>();
                await foreach (var row in stream)
                {
                    if (row.Key.ToStringUtf8().StartsWith(normalizedPrefix, StringComparison.Ordinal))
                    {
                        mutateRequests.Add(new MutateRowsRequest
                        {
                            TableNameAsTableName = tableName,
                            Entries =
                            {
                                new MutateRowsRequest.Types.Entry
                                {
                                    RowKey = row.Key,
                                    Mutations = { new Mutation { DeleteFromRow = new Mutation.Types.DeleteFromRow() } }
                                }
                            }
                        });
                    }
                }
                foreach (var req in mutateRequests)
                {
                    await _bigtableClient.MutateRowsAsync(req);
                }

                // Delete from GCS
                var objects = _storageClient.ListObjectsAsync(_bucketName, normalizedPrefix);
                await foreach (var obj in objects)
                {
                    _logger.LogDebug("Deleting GCS object {objectName}", obj.Name);
                    await _storageClient.DeleteObjectAsync(obj);
                }
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete folder {prefix}", prefix);
                SetUnhealthy(ex);
                throw new InvalidOperationException("Hybrid storage unavailable", ex);
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                // Try Bigtable first
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var readRowsRequest = new ReadRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Rows = new RowSet(),
                    Filter = new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("content") }
                };
                readRowsRequest.Rows.RowKeys.Add(ByteString.CopyFromUtf8(objectKey));
                var stream = _bigtableClient.ReadRows(readRowsRequest);
                await foreach (var row in stream)
                {
                    if (row.Key.ToStringUtf8() == objectKey && row.Families.Count > 0)
                    {
                        var family = row.Families[0];
                        if (family.Columns.Count > 0)
                        {
                            var cell = family.Columns[0].Cells[0];
                            SetHealthy();
                            return new ReadOnlyMemory<byte>(cell.Value.ToByteArray());
                        }
                    }
                }

                // Fallback to GCS
                using var streamGcs = new MemoryStream();
                await _storageClient.DownloadObjectAsync(_bucketName, objectKey, streamGcs);
                SetHealthy();
                return new ReadOnlyMemory<byte>(streamGcs.ToArray());
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                throw new FileNotFoundException("File not found", objectKey, e);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            await EnsureInitialized();
            try
            {
                // For Bigtable, if it's there, download full and slice
                var full = await DownloadBytes(objectKey);
                if (position + length > full.Length)
                    throw new ArgumentOutOfRangeException();
                return full.Slice((int)position, length);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            await EnsureInitialized();
            var pipe = new Pipe();

            async Task Produce()
            {
                try
                {
                    // Try Bigtable
                    var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                    var readRowsRequest = new ReadRowsRequest
                    {
                        TableNameAsTableName = tableName,
                        Rows = new RowSet(),
                        Filter = new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("content") }
                    };
                    readRowsRequest.Rows.RowKeys.Add(ByteString.CopyFromUtf8(objectKey));
                    var stream = _bigtableClient.ReadRows(readRowsRequest);
                    bool found = false;
                    await foreach (var row in stream)
                    {
                        if (row.Key.ToStringUtf8() == objectKey && row.Families.Count > 0)
                        {
                            var family = row.Families[0];
                            if (family.Columns.Count > 0)
                            {
                                var cell = family.Columns[0].Cells[0];
                                using var ms = new MemoryStream(cell.Value.ToByteArray());
                                await ms.CopyToAsync(pipe.Writer.AsStream(true));
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found)
                    {
                        // GCS
                        await _storageClient.DownloadObjectAsync(_bucketName, objectKey, pipe.Writer.AsStream(true));
                    }
                    await pipe.Writer.CompleteAsync();
                    SetHealthy();
                }
                catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    SetHealthy();
                    await pipe.Writer.CompleteAsync(new FileNotFoundException("File not found", objectKey, e));
                }
                catch (Exception e)
                {
                    SetUnhealthy(e);
                    await pipe.Writer.CompleteAsync(e);
                }
            }

            var producer = Produce();
            callback(pipe.Reader.AsStream(true));
            await producer;
        }

        public async Task<long> GetObjectContentLength(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                // Try Bigtable
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var readRowsRequest = new ReadRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Rows = new RowSet(),
                    Filter = new RowFilter { ColumnQualifierRegexFilter = ByteString.CopyFromUtf8("content") }
                };
                readRowsRequest.Rows.RowKeys.Add(ByteString.CopyFromUtf8(objectKey));
                var stream = _bigtableClient.ReadRows(readRowsRequest);
                await foreach (var row in stream)
                {
                    if (row.Key.ToStringUtf8() == objectKey && row.Families.Count > 0)
                    {
                        var family = row.Families[0];
                        if (family.Columns.Count > 0)
                        {
                            var cell = family.Columns[0].Cells[0];
                            SetHealthy();
                            return cell.Value.Length;
                        }
                    }
                }

                // GCS
                var obj = await _storageClient.GetObjectAsync(_bucketName, objectKey);
                SetHealthy();
                return (long)obj.Size.GetValueOrDefault();
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                throw new FileNotFoundException("File not found", objectKey, e);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            await EnsureInitialized();
            try
            {
                var result = new List<(string ObjectKey, DateTimeOffset DateTime)>();

                // Bigtable
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var readRowsRequest = new ReadRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Rows = new RowSet(),
                    RowsLimit = 10000 // Adjust as needed
                };
                if (!string.IsNullOrEmpty(prefix))
                {
                    readRowsRequest.Rows.RowRanges.Add(new RowRange { StartKeyClosed = ByteString.CopyFromUtf8(prefix) });
                }
                var stream = _bigtableClient.ReadRows(readRowsRequest);
                await foreach (var row in stream)
                {
                    var key = row.Key.ToStringUtf8();
                    if (string.IsNullOrEmpty(prefix) || key.StartsWith(prefix, StringComparison.Ordinal))
                    {
                        result.Add((key, DateTimeOffset.UtcNow)); // Bigtable doesn't have timestamps easily, use now
                    }
                }

                // GCS
                var objects = _storageClient.ListObjectsAsync(_bucketName, prefix);
                await foreach (var obj in objects)
                {
                    result.Add((obj.Name, obj.UpdatedDateTimeOffset.GetValueOrDefault()));
                }

                SetHealthy();
                return result.DistinctBy(x => x.ObjectKey).ToList(); // Remove duplicates if any
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task UploadBytes(string objectKey, byte[] bytes)
        {
            await EnsureInitialized();
            if (bytes.Length < _sizeThresholdBytes)
            {
                // Upload to Bigtable
                var tableName = new TableName(_bigtableProjectId, _bigtableInstanceId, _bigtableTableId);
                var mutateRowsRequest = new MutateRowsRequest
                {
                    TableNameAsTableName = tableName,
                    Entries =
                    {
                        new MutateRowsRequest.Types.Entry
                        {
                            RowKey = ByteString.CopyFromUtf8(objectKey),
                            Mutations =
                            {
                                new Mutation
                                {
                                    SetCell = new Mutation.Types.SetCell
                                    {
                                        FamilyName = "data",
                                        ColumnQualifier = ByteString.CopyFromUtf8("content"),
                                        TimestampMicros = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000,
                                        Value = ByteString.CopyFrom(bytes)
                                    }
                                }
                            }
                        }
                    }
                };
                await _bigtableClient.MutateRowsAsync(mutateRowsRequest);
            }
            else
            {
                // Upload to GCS
                using var stream = new MemoryStream(bytes);
                await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
            }
            SetHealthy();
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            await EnsureInitialized();
            // For streams, we need to check size, but streams might not have length, so assume large or buffer
            // For simplicity, since deltadata is stream, and can be large, upload to GCS
            await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
            SetHealthy();
        }

        private void SetHealthy()
        {
            _lastResult = (DateTimeOffset.UtcNow, null);
        }

        private void SetUnhealthy(Exception ex)
        {
            _lastResult = (DateTimeOffset.UtcNow, ex);
        }

        public void Dispose()
        {
            _storageClient?.Dispose();
        }
    }
}