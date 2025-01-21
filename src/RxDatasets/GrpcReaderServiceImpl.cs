using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets;

public class GrpcReaderServiceImpl<TEntity>(ILogger<GrpcReaderServiceImpl<TEntity>> logger, IHostApplicationLifetime appLifetime, IConfiguration config, IDatasetReader<TEntity> dataset)
    where TEntity : class, IMessage<TEntity>
{
    public record struct ListChangesRequest(ulong FromDatasetVersion, int PageSize, string PageToken, bool WaitForChanges);
    public record struct ListChangesResponse(ulong ToDatasetVersion, IEnumerable<string> NewEntities, IEnumerable<string> ChangedEntities, IEnumerable<string> DeletedEntities, string NextPageToken);

    private readonly int _paginationTimeOut = config.GetValue("PaginationCacheDuration", 60);
    private readonly MessageParser<TEntity> _protoParser = new(() => (TEntity)Activator.CreateInstance(typeof(TEntity))!);
    private readonly IMemoryCache _memoryCache = new MemoryCache(new MemoryCacheOptions()); 

    private CancellationTokenSource CombiTokenSource(CancellationToken contextToken)
    {
        return CancellationTokenSource.CreateLinkedTokenSource(contextToken, appLifetime.ApplicationStopping);
    }

    public async Task<ICollection<TEntity>> BatchGet(IEnumerable<string> listOfNames, string? requestParent,
        CancellationToken cancellationToken)
    {
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            var entities = await dataset
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

    public async Task<TEntity> Get(string name, CancellationToken cancellationToken)
    {
        var entity = await dataset.Get(name) ?? throw new RpcException(new Status(StatusCode.NotFound,
                $"The requested {typeof(TEntity).Name} does not exist: {name}"));

        return _protoParser.ParseFrom(entity.entityBytes.Span);
    }

    public async Task<(ICollection<TEntity> batch, string? nextPageToken)> List(string parent, int pageSize, string pageToken, CancellationToken cancellationToken)
    {
        var isPaging = pageToken?.Length > 0;
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            var (names, toDatasetVersion) = isPaging ? PullRemainder<string>(pageToken) : await dataset
                .List(parent);
            
            var (paginatedNames, nextPageToken, responseToDatasetVersion) = Paginate(names, pageSize, toDatasetVersion);

            var batch = await BatchGet(paginatedNames, parent, cts.Token);
        

            return (batch, nextPageToken);
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
            return (Array.Empty<TEntity>(), null);
        }
    }
    
    public async Task<ListChangesResponse> ListChanges(ListChangesRequest request, CancellationToken cancellationToken)
    {
        using var cts = CombiTokenSource(cancellationToken);
        try
        {
            var isPaging = request.PageToken?.Length > 0;
            var (allChanges, toDatasetVersion) = isPaging
                    ? PullRemainder<(string name, ChangeType changeType)>(request.PageToken!) 
                    : request.WaitForChanges 
                        ? await WaitChanges(request.FromDatasetVersion, cts.Token) 
                        : await GetChanges(request.FromDatasetVersion, cts.Token);

            var (changes, nextPageToken, responseToDatasetVersion) = Paginate(allChanges, request.PageSize, toDatasetVersion);
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

    private async Task<(ICollection<(string name, ChangeType changeType)> allChanges, ulong toDatasetVersion)> GetChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.GetChangedNames(requestFromDatasetVersion);
        return (changes.changes, changes.toVersion);
    }

    private async Task<(ICollection<(string name, ChangeType changeType)> allChanges, ulong toDatasetVersion)> WaitChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.WaitChangedNames(requestFromDatasetVersion, ctsToken);
        return (changes.changes, changes.toVersion);
    }

    private (ICollection<T> Changes, string NextPageToken, ulong ResponseToDatasetVersion) Paginate<T>(ICollection<T> allChanges, int requestPageSize, ulong toDatasetVersion)
    {
        if (requestPageSize == 0)
            requestPageSize = _defaultPageSize;

        if (requestPageSize > _maxPageSize)
            requestPageSize = _maxPageSize;

        if (allChanges.Count <= requestPageSize)
            return (allChanges, string.Empty, toDatasetVersion);

        var thisPage = allChanges.Take(requestPageSize).ToList();
        var remainder = allChanges.Skip(requestPageSize).ToList();
        var nextPageToken = Guid.NewGuid().ToString().ToLowerInvariant();
        StoreRemainder(nextPageToken, remainder,toDatasetVersion);
        return (thisPage, nextPageToken, 0);
    }

    private void StoreRemainder<T>(string key, ICollection<T> collection, ulong toDatasetVersion)
    {
        _memoryCache.Set(key, (collection, toDatasetVersion), TimeSpan.FromSeconds(_paginationTimeOut));
    }

    private (ICollection<T> Remainder, ulong ToDatasetVersion) PullRemainder<T>(string key)
    {
        var cacheValueFound = _memoryCache.TryGetValue(key, out (ICollection<T> collection, ulong ToDatasetVersion) cacheValue);
        _memoryCache.Remove(key);
        if (!cacheValueFound || cacheValue.collection is not ICollection<T> remainder)
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Requested nextPageToken does not exist or is no longer valid"));
        logger.LogInformation($"Cache hit for {key} in dataset reader");
        return (cacheValue.collection, cacheValue.ToDatasetVersion);
    }

    private static int? AsInt(string? config) => config switch
    {
        null => null,
        not null => int.TryParse(config, out var intValue) ? intValue : null
    };


    private readonly int _defaultPageSize = AsInt(config["PageSize"]) ?? 1000;
    private readonly int _maxPageSize = AsInt(config["MaxPageSize"]) ?? 1000;
}