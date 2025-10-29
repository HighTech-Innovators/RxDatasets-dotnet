using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets;

public class GrpcReaderServiceImpl<TEntity>(ILogger<GrpcReaderServiceImpl<TEntity>> logger, IHostApplicationLifetime appLifetime, IConfiguration config, IDatasetReader<TEntity> dataset, IServiceProvider serviceProvider)
    where TEntity : class, IMessage<TEntity>
{
    public record struct ListChangesRequest(ulong FromDatasetVersion, int PageSize, string PageToken, bool WaitForChanges);
    public record struct ListChangesResponse(ulong ToDatasetVersion, IEnumerable<string> NewEntities, IEnumerable<string> ChangedEntities, IEnumerable<string> DeletedEntities, string NextPageToken);
    public async Task<ICollection<TEntity>> BatchGet(IEnumerable<string> listOfNames, string? requestParent,
        CancellationToken cancellationToken) => await BatchGet(listOfNames, requestParent, "", cancellationToken);

    public async Task<ICollection<TEntity>> BatchGet(IEnumerable<string> listOfNames, string? requestParent,
        string aliasName, CancellationToken cancellationToken)
    {
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            var entities = await GetDataset(aliasName)
                .BatchGet(listOfNames, requestParent);
            return entities.Where(entity => entity.entityBytes is not null)
                .Select(item => _protoParser.ParseFrom(item.entityBytes!.Value.Span)).ToList();
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            if (appLifetime.ApplicationStopping.IsCancellationRequested)
            {
                // Server is stopping, notify the client
                throw new RpcException(new Status(StatusCode.Unavailable, "Server is shutting down. Operation was aborted."));
            }
            // For client-initiated cancellations, you don't necessarily need to throw an RpcException
            // because the client already knows it cancelled the operation.
            return Array.Empty<TEntity>();
        }
    }

    public async Task<TEntity> Get(string name, CancellationToken cancellationToken) => await Get(name, "", cancellationToken);

    public async Task<TEntity> Get(string name, string aliasName, CancellationToken cancellationToken)
    {
        var entity = await GetDataset(aliasName).Get(name) ?? throw new RpcException(new Status(StatusCode.NotFound,
                $"The requested {typeof(TEntity).Name} does not exist: {name}"));

        return _protoParser.ParseFrom(entity.entityBytes.Span);
    }

    /// <summary>
    /// Lists child entities for the given parent.
    /// Deprecated: Use ListChildren to get IDs/names instead, then BatchGet to retrieve entities.
    /// </summary>
    [Obsolete("Use GetChildren instead")]
    public async Task<(ICollection<TEntity> batch, string? nextPageToken)> List(string parent, int pageSize, string pageToken, CancellationToken cancellationToken) => await GetChildren(parent, pageSize, pageToken, cancellationToken);

    /// <summary>
    /// Lists child entities for the given parent.
    /// Deprecated: Use ListChildren to get IDs/names instead, then BatchGet to retrieve entities.
    /// </summary>
    [Obsolete("Use GetChildren instead")]
    public async Task<(ICollection<TEntity> batch, string? nextPageToken)> List(string parent, int pageSize, string pageToken, string aliasName, CancellationToken cancellationToken) => await GetChildren(parent, pageSize, pageToken, aliasName, cancellationToken);

    /// <summary>
    /// Lists child entities for the given parent.
    /// </summary>
    public async Task<(ICollection<string> childrenIds, string? nextPageToken)> ListChildren(string parent, int pageSize, string pageToken, CancellationToken cancellationToken) => await ListChildren(parent, pageSize, pageToken, "", cancellationToken);

    /// <summary>
    /// Lists child entities for the given parent.
    /// </summary>
    public async Task<(ICollection<string> childrenIds, string? nextPageToken)> ListChildren(string parent, int pageSize, string pageToken, string aliasName, CancellationToken cancellationToken)
    {
        var datasetReader = GetDataset(aliasName);
        var isPaging = pageToken?.Length > 0;
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            List<string>? names;
            var alreadySkipped = 0;
            ulong toDatasetVersion;

            if (isPaging)
            {
                (names, alreadySkipped, toDatasetVersion) = PullRemainder<string>(pageToken!);
            }
            else
            {
                (names, toDatasetVersion) = await datasetReader.List(parent, cts.Token);
            }

            var (paginatedNames, nextPageToken, _) = Paginate(names, pageSize, alreadySkipped, toDatasetVersion);

            return (paginatedNames, nextPageToken);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            if (appLifetime.ApplicationStopping.IsCancellationRequested)
            {
                // Server is stopping, notify the client
                throw new RpcException(new Status(StatusCode.Unavailable, "Server is shutting down. Operation was aborted."));
            }
            // For client-initiated cancellations, you don't necessarily need to throw an RpcException
            // because the client already knows it cancelled the operation.
            return (Array.Empty<string>(), null);
        }
    }

    /// <summary>
    /// Gets child entities for the given parent.
    /// This method combines ListChildren and BatchGet to retrieve the full entities.
    /// </summary>
    public async Task<(ICollection<TEntity> batch, string? nextPageToken)> GetChildren(string parent, int pageSize, string pageToken, CancellationToken cancellationToken) => await GetChildren(parent, pageSize, pageToken, "", cancellationToken);

    /// <summary>
    /// Gets child entities for the given parent.
    /// This method combines ListChildren and BatchGet to retrieve the full entities.
    /// </summary>
    public async Task<(ICollection<TEntity> batch, string? nextPageToken)> GetChildren(string parent, int pageSize, string pageToken, string aliasName, CancellationToken cancellationToken)
    {
        using var cts = CombiTokenSource(cancellationToken);
        var (childrenIds, nextPageToken) = await ListChildren(parent, pageSize, pageToken, aliasName, cts.Token);
        var batch = await BatchGet(childrenIds, parent, aliasName, cts.Token);
        return (batch, nextPageToken);
    }


    public async Task<uint> CountChildren(string parent, CancellationToken cancellationToken) => await CountChildren(parent, "", cancellationToken);

    public async Task<uint> CountChildren(string parent, string aliasName, CancellationToken cancellationToken)
    {
        var datasetReader = GetDataset(aliasName);
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            logger.LogDebug("CountChildren called for parent: {Parent}", parent ?? "<null>");

            var count = await datasetReader.CountChildren(parent ?? "", cts.Token);

            logger.LogDebug("CountChildren returned count {Count} for parent {Parent}", count, parent ?? "<null>");

            return count;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            if (appLifetime.ApplicationStopping.IsCancellationRequested)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "Server is shutting down. Operation was aborted."));
            }
            throw;
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public async Task<ListChangesResponse> ListChanges(ListChangesRequest request, CancellationToken cancellationToken)
    {
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            var isPaging = request.PageToken?.Length > 0;
            List<(string name, ChangeType changeType)>? allChanges;
            var alreadySkipped = 0;
            ulong toDatasetVersion;

            if (isPaging)
            {
                (allChanges, alreadySkipped, toDatasetVersion) =
                    PullRemainder<(string name, ChangeType changeType)>(request.PageToken!);
            }
            else
            {
                var allChangesCollection = request.WaitForChanges ? await WaitChanges(request.FromDatasetVersion, cts.Token)
                    : await GetChanges(request.FromDatasetVersion, cts.Token);
                allChanges = allChangesCollection.allChanges;
                toDatasetVersion = allChangesCollection.toDatasetVersion;
            }

            var (changes, nextPageToken, responseToDatasetVersion) = Paginate(allChanges, request.PageSize, alreadySkipped, toDatasetVersion);
            return new ListChangesResponse
            {
                ToDatasetVersion = responseToDatasetVersion,
                NewEntities = changes.Where(ch => ch.changeType == ChangeType.New).Select(ch => ch.name),
                ChangedEntities = changes.Where(ch => ch.changeType == ChangeType.Changed).Select(ch => ch.name),
                DeletedEntities = changes.Where(ch => ch.changeType == ChangeType.Deleted).Select(ch => ch.name),
                NextPageToken = nextPageToken
            };
        }
        catch (ArgumentOutOfRangeException)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "fromDatasetVersion is newer than latest DatasetVersion"));
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            if (appLifetime.ApplicationStopping.IsCancellationRequested)
            {
                // Server is stopping, notify the client
                throw new RpcException(new Status(StatusCode.Unavailable, "Server is shutting down. Operation was aborted."));
            }
            // For client-initiated cancellations, you don't necessarily need to throw an RpcException
            // because the client already knows it cancelled the operation.
            return new ListChangesResponse();
        }
    }

    private readonly int _paginationTimeOut = config.GetValue("PaginationCacheDuration", 60);
    private readonly MessageParser<TEntity> _protoParser = new(() => (TEntity)Activator.CreateInstance(typeof(TEntity))!);
    private readonly MemoryCache _memoryCache = new(new MemoryCacheOptions());

    private CancellationTokenSource CombiTokenSource(CancellationToken contextToken) => CancellationTokenSource.CreateLinkedTokenSource(contextToken, appLifetime.ApplicationStopping);

    /// <summary>
    /// Gets the dataset reader for the given alias name.
    /// If aliasName is empty or null, returns the default dataset.
    /// </summary>
    /// <param name="aliasName">The alias name of the dataset. Empty string returns the default dataset.</param>
    /// <returns>The dataset reader for the given alias.</returns>
    /// <exception cref="RpcException">Thrown when the dataset alias is invalid or cannot be resolved.</exception>
    private IDatasetReader<TEntity> GetDataset(string aliasName = "")
    {
        if (string.IsNullOrEmpty(aliasName))
        {
            return dataset;
        }

        var aliasedDataset = serviceProvider!.GetKeyedService<IDatasetReader<TEntity>>(aliasName);
        if (aliasedDataset == null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"Dataset alias '{aliasName}' not found"));
        }

        return aliasedDataset;
    }

    private async Task<(List<(string name, ChangeType changeType)> allChanges, ulong toDatasetVersion)> GetChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.GetChangedNames(requestFromDatasetVersion);
        return (changes.Changes, changes.ToVersion);
    }

    private async Task<(List<(string name, ChangeType changeType)> allChanges, ulong toDatasetVersion)> WaitChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.WaitChangedNames(requestFromDatasetVersion, ctsToken);
        return (changes.Changes, changes.ToVersion);
    }

    private (List<T> Changes, string NextPageToken, ulong ResponseToDatasetVersion) Paginate<T>(List<T> allChanges, int requestPageSize, int skipSize, ulong toDatasetVersion)
        => PaginationHelper.Paginate(allChanges, requestPageSize, skipSize, toDatasetVersion, _defaultPageSize, _maxPageSize, StoreRemainder);

    private void StoreRemainder<T>(string key, List<T> collection, int alreadyReadSize, ulong toDatasetVersion) => _memoryCache.Set(key, (collection, alreadyReadSize, toDatasetVersion), TimeSpan.FromSeconds(_paginationTimeOut));

    private (List<T> Remainder, int AlreadySkipped, ulong ToDatasetVersion) PullRemainder<T>(string key)
    {
        var cacheValueFound = _memoryCache.TryGetValue(key, out (List<T> collection, int alreadySkipped, ulong ToDatasetVersion) cacheValue);
        _memoryCache.Remove(key);
        if (!cacheValueFound || cacheValue.collection is not List<T>)
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Requested nextPageToken does not exist or is no longer valid"));
        logger.LogDebug($"Cache hit for {key} in dataset reader");
        return (cacheValue.collection, cacheValue.alreadySkipped, cacheValue.ToDatasetVersion);
    }

    private static int? AsInt(string? config) => config switch
    {
        null => null,
        not null => int.TryParse(config, out var intValue) ? intValue : null
    };


    private readonly int _defaultPageSize = AsInt(config["PageSize"]) ?? 1000;
    private readonly int _maxPageSize = AsInt(config["MaxPageSize"]) ?? 1000;
}
