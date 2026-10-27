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
    public record struct ListChangesResponse(ulong ToDatasetVersion, IEnumerable<string> NewEntities, IEnumerable<string> ChangedEntities, IEnumerable<string> DeletedEntities, string NextPageToken)
    {
        private readonly IReadOnlyDictionary<string, ChangeOrigin>? _origins;

        /// <summary>
        /// The origin of the latest change of each name on this page whose write passed a tracking id or an SLA
        /// timestamp; names whose write passed neither are absent.
        /// </summary>
        public IReadOnlyDictionary<string, ChangeOrigin> Origins
        {
            get => _origins ?? NoOrigins;
            init => _origins = value;
        }

        private static readonly IReadOnlyDictionary<string, ChangeOrigin> NoOrigins = new Dictionary<string, ChangeOrigin>();
    }
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

    public async Task<TEntity> GetHistoric(string name, ulong version, CancellationToken cancellationToken)
    {
        try
        {
            var entity = await dataset.GetHistoric(name, version, cancellationToken);
            if (entity == null)
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"The requested {typeof(TEntity).Name} does not exist in version {version}: {name}"));
            }
            return _protoParser.ParseFrom(entity.Value.entityBytes.Span);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
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

            var scope = new ListingScope(parent, aliasName);
            if (isPaging)
            {
                (names, alreadySkipped, toDatasetVersion) = PullRemainder<string>(pageToken!, scope);
            }
            else
            {
                (names, toDatasetVersion) = await datasetReader.List(parent, cts.Token);
            }

            var (paginatedNames, nextPageToken, _) = Paginate(names, pageSize, alreadySkipped, toDatasetVersion, scope);

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
            List<(string name, ChangeType changeType, ChangeOrigin? origin)>? allChanges;
            var alreadySkipped = 0;
            ulong toDatasetVersion;

            if (isPaging)
            {
                (allChanges, alreadySkipped, toDatasetVersion) =
                    PullRemainder<(string name, ChangeType changeType, ChangeOrigin? origin)>(request.PageToken!, ListingScope.WholeDataset);
            }
            else
            {
                var allChangesCollection = request.WaitForChanges ? await WaitChanges(request.FromDatasetVersion, cts.Token)
                    : await GetChanges(request.FromDatasetVersion, cts.Token);
                allChanges = allChangesCollection.allChanges;
                toDatasetVersion = allChangesCollection.toDatasetVersion;
            }

            var (changes, nextPageToken, responseToDatasetVersion) =
                Paginate(allChanges, request.PageSize, alreadySkipped, toDatasetVersion, ListingScope.WholeDataset);
            return new ListChangesResponse
            {
                ToDatasetVersion = responseToDatasetVersion,
                NewEntities = changes.Where(ch => ch.changeType == ChangeType.New).Select(ch => ch.name),
                ChangedEntities = changes.Where(ch => ch.changeType == ChangeType.Changed).Select(ch => ch.name),
                DeletedEntities = changes.Where(ch => ch.changeType == ChangeType.Deleted).Select(ch => ch.name),
                NextPageToken = nextPageToken,
                Origins = changes.Where(ch => ch.origin != null).ToDictionary(ch => ch.name, ch => ch.origin!)
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
            // because the client already knows it cancelled the operation. The lists are empty rather
            // than null: a default record struct would leave them null, and callers map them.
            return new ListChangesResponse
            {
                NewEntities = [],
                ChangedEntities = [],
                DeletedEntities = [],
                NextPageToken = ""
            };
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

    private async Task<(List<(string name, ChangeType changeType, ChangeOrigin? origin)> allChanges, ulong toDatasetVersion)> GetChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.GetChangedNames(requestFromDatasetVersion);
        return (WithOrigins(changes), changes.ToVersion);
    }

    private async Task<(List<(string name, ChangeType changeType, ChangeOrigin? origin)> allChanges, ulong toDatasetVersion)> WaitChanges(ulong requestFromDatasetVersion,
        CancellationToken ctsToken)
    {
        var changes = await dataset.WaitChangedNames(requestFromDatasetVersion, ctsToken);
        return (WithOrigins(changes), changes.ToVersion);
    }

    /// <summary>The changes with their origins beside them, so that later pages, served from the cache, keep theirs.</summary>
    private static List<(string name, ChangeType changeType, ChangeOrigin? origin)> WithOrigins(ChangeSet changes) =>
        [.. changes.Changes.Select(c => (c.Name, c.ChangeType, changes.Origins.GetValueOrDefault(c.Name)))];

    /// <summary>
    /// What a listing was asked for, beyond its paging. A page token continues only the listing it was issued for:
    /// the remainder is served from the cache and not read again, so a follow-up call with another parent or alias
    /// would otherwise get the rest of a listing it never asked for — past any check made on what it did send.
    /// </summary>
    /// <remarks>
    /// <c>ListChanges</c> always covers the whole dataset, and its <c>FromDatasetVersion</c> is ignored on later
    /// pages by design, so it has nothing to bind.
    /// </remarks>
    private readonly record struct ListingScope(string Parent, string AliasName)
    {
        public static readonly ListingScope WholeDataset = new("", "");
    }

    private (List<T> Changes, string NextPageToken, ulong ResponseToDatasetVersion) Paginate<T>(List<T> allChanges, int requestPageSize, int skipSize,
        ulong toDatasetVersion, ListingScope scope)
        => PaginationHelper.Paginate(allChanges, requestPageSize, skipSize, toDatasetVersion, _defaultPageSize, _maxPageSize,
            (key, collection, alreadyReadSize, version) => StoreRemainder(key, collection, alreadyReadSize, version, scope));

    private void StoreRemainder<T>(string key, List<T> collection, int alreadyReadSize, ulong toDatasetVersion, ListingScope scope)
        => _memoryCache.Set(key, (collection, alreadyReadSize, toDatasetVersion, scope), TimeSpan.FromSeconds(_paginationTimeOut));

    private (List<T> Remainder, int AlreadySkipped, ulong ToDatasetVersion) PullRemainder<T>(string key, ListingScope scope)
    {
        var cacheValueFound = _memoryCache.TryGetValue(key,
            out (List<T> collection, int alreadySkipped, ulong ToDatasetVersion, ListingScope Scope) cacheValue);
        _memoryCache.Remove(key);
        if (!cacheValueFound || cacheValue.collection is not List<T>)
        {
            logger.LogWarning("Requested nextPageToken is no longer valid or does not exist. PageToken={PageToken}, EntityType={EntityType}", key, typeof(TEntity).Name);
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Requested nextPageToken does not exist or is no longer valid"));
        }
        if (cacheValue.Scope != scope)
        {
            logger.LogWarning("Requested nextPageToken belongs to another listing. PageToken={PageToken}, EntityType={EntityType}", key, typeof(TEntity).Name);
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Requested nextPageToken does not belong to this request"));
        }
        logger.LogDebug($"Cache hit for {key} in dataset reader");
        return (cacheValue.collection, cacheValue.alreadySkipped, cacheValue.ToDatasetVersion);
    }

    private static int? AsInt(string? config) => config switch
    {
        null => null,
        not null => int.TryParse(config, out var intValue) ? intValue : null
    };

    public async Task<List<HistoryEntry>> GetHistory(string id, string aliasName, int limit, ulong minVersion, CancellationToken cancellationToken)
    {
        return await GetDataset(aliasName).GetHistory(id, limit, minVersion, cancellationToken);
    }

    public IAsyncEnumerable<HistoryEntry> StreamHistory(string parent, string aliasName, ulong sinceVersion, ulong maxVersion, bool waitIfNone, CancellationToken cancellationToken)
    {
        return GetDataset(aliasName).StreamHistory(parent, sinceVersion, maxVersion, waitIfNone, cancellationToken);
    }

    public IAsyncEnumerable<HistoryEntry> TailHistory(string parent, string aliasName, ulong sinceVersion, bool follow, CancellationToken cancellationToken)
    {
        return GetDataset(aliasName).TailHistory(parent, sinceVersion, follow, cancellationToken);
    }

    private readonly int _defaultPageSize = AsInt(config["PageSize"]) ?? 1000;
    private readonly int _maxPageSize = AsInt(config["MaxPageSize"]) ?? 1000;
}
