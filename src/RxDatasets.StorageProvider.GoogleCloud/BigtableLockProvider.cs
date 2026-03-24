using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class BigtableLockProvider<TEntity>(
    IVersionedArtifactRowStore rowStore,
    IOptions<StorageConfig> storageConfig,
    ILogger<BigtableLockProvider<TEntity>> logger) : ILockProvider, IDisposable
    where TEntity : class, IMessage<TEntity>
{
    private readonly ILogger<BigtableLockProvider<TEntity>> _logger = logger;
    private bool _disposed;

    public async Task<(string Token, DateTimeOffset Expiry)?> AcquireLockAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            var result = await rowStore.TryAcquireLock(datasetPrefix, lockName, duration, cancellationToken);
            if (result != null)
            {
                _logger.LogDebug("Acquired lock {LockName} with token {Token}", lockName, result.Value.Token);
            }
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to acquire lock {LockName}", lockName);
            return null;
        }
    }

    public async Task<bool> RenewLockAsync(string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            var success = await rowStore.RenewLock(datasetPrefix, lockName, lockToken, duration, cancellationToken);
            if (success)
            {
                _logger.LogDebug("Renewed lock {LockName}", lockName);
            }
            else
            {
                _logger.LogWarning("Failed to renew lock {LockName} (token mismatch or expired)", lockName);
            }
            return success;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to renew lock {LockName}", lockName);
            return false;
        }
    }

    public async Task ReleaseLockAsync(string lockName, string lockToken, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            await rowStore.ReleaseLock(datasetPrefix, lockName, lockToken, cancellationToken);
            _logger.LogDebug("Released lock {LockName}", lockName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to release lock {LockName}", lockName);
        }
    }

    public async Task RequestLockAsync(string requestLockName, string instanceId, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            await rowStore.RequestLock(datasetPrefix, requestLockName, instanceId, cancellationToken);
            _logger.LogDebug("Requested lock {LockName} for instance {InstanceId}", requestLockName, instanceId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to request lock {LockName}", requestLockName);
        }
    }

    public async Task<bool> ShouldYieldAsync(string requestLockName, string instanceId, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            return await rowStore.ShouldYield(datasetPrefix, requestLockName, instanceId, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to check yield status for {LockName}", requestLockName);
            return false;
        }
    }

    public async Task DeleteRequestLockAsync(string requestLockName, CancellationToken cancellationToken)
    {
        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value);
        try
        {
            await rowStore.DeleteRequestLock(datasetPrefix, requestLockName, cancellationToken);
            _logger.LogDebug("Deleted request lock {LockName}", requestLockName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete request lock {LockName}", requestLockName);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }
}
