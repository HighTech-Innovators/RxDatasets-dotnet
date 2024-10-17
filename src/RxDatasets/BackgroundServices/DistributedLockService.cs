using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders;

namespace RxDatasets.BackgroundServices
{
    public class DistributedLockService<TEntity> : IHostedService, IDisposable
    {
        private readonly TimeSpan _renewalInterval = TimeSpan.FromMinutes(5); // Interval to attempt lock renewal
        private Timer? _renewalTimer;
        private string? _lockToken;
        private bool _isDisposed = false;
        private readonly IObjectStorageProvider<TEntity> _storageProvider;
        private readonly IOptions<StorageConfig> storageConfig;
        private readonly ILogger<DistributedLockService<TEntity>> _logger;
        private readonly INotificationsHub<TEntity> notificationsHub;
        private readonly string _lockName;

        public DistributedLockService(IObjectStorageProvider<TEntity> storageProvider, IOptions<StorageConfig> storageConfig
        , ILogger<DistributedLockService<TEntity>> logger, INotificationsHub<TEntity> notificationsHub
        )
        {
            _storageProvider = storageProvider;
            this.storageConfig = storageConfig;
            _logger = logger;
            this.notificationsHub = notificationsHub;
            _lockName = ObjectStorageProvider<TEntity>.GetObjectNamePrefix(storageConfig.Value) + "lock-file";

    }

    public bool HasLock => !string.IsNullOrEmpty(_lockToken);

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Starting DistributedLockService...");
            Task.Run(async () =>
            {
                await AttemptAcquireLockAsync();

                while (!HasLock && !cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning("Lock is currently held by another process. checking again in 5 seconds");
                    await Task.Delay(TimeSpan.FromSeconds(5));
                    await AttemptAcquireLockAsync();
                }
            });

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping DistributedLockService...");

            _renewalTimer?.Change(Timeout.Infinite, 0);
            _logger.LogInformation("Application is stopping. Releasing lock...");

            try
            {
                // Ensure the lock is released before the application exits
                await ReleaseLockAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error releasing lock during application stopping.");
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            _renewalTimer?.Dispose();
            _isDisposed = true;
        }

        private async Task AttemptAcquireLockAsync()
        {
            if (HasLock)
            {
                return; // Already have the lock
            }

            _logger.LogInformation("Attempting to acquire lock...");
            _lockToken = await AcquireLockAsync();

            if (HasLock)
            {
                _logger.LogInformation("Lock acquired.");
                TimeSpan renewAfterTime = _renewalInterval - TimeSpan.FromSeconds(30);
                _renewalTimer = new Timer(async _ => await RenewLockAsync(), null, renewAfterTime, renewAfterTime);
            }
            else
            {
                _logger.LogWarning("Failed to acquire lock.");
            }
        }

        /// <summary>
        ///  Does not throw exceptions
        /// </summary>
        /// <returns></returns>
        private async Task<string?> AcquireLockAsync()
        {
            string lockKey = _lockName;
            string lockToken = Guid.NewGuid().ToString();

            try
            {
                var existingLock = await TryReadLockFile(lockKey);

                if (existingLock == null || existingLock.Value.Expiry < DateTimeOffset.UtcNow)
                {
                    // No existing lock or the lock has expired, acquire a new lock
                    DateTimeOffset expiryTime = DateTimeOffset.UtcNow.Add(_renewalInterval);
                    await WriteLockFile(lockKey, lockToken, expiryTime);
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Acquried));
                    return lockToken;
                }

                // Lock is still valid
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error acquiring lock.");
                return null;
            }
        }

        private async Task RenewLockAsync()
        {
            if (!HasLock)
            {
                _logger.LogWarning("No lock to renew.");
                return;
            }

            string lockKey = _lockName;

            try
            {
                var existingLock = await TryReadLockFile(lockKey);

                if ((existingLock != null && existingLock.Value.Token == _lockToken) || existingLock?.Expiry < DateTimeOffset.UtcNow)
                {
                    DateTimeOffset expiryTime = DateTimeOffset.UtcNow.Add(_renewalInterval);
                    await WriteLockFile(lockKey, _lockToken!, expiryTime);
                    _logger.LogInformation("Lock renewed.");
                }
                else
                {
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                    _logger.LogWarning("Cannot renew lock not owned by this instance.");
                    _renewalTimer?.Change(Timeout.Infinite, 0);
                    _renewalTimer?.Dispose();
                    _renewalTimer = null;
                }
            }
            catch (Exception ex)
            {
                await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                _logger.LogError(ex, "Error renewing lock.");
                _renewalTimer?.Change(Timeout.Infinite, 0);
                _renewalTimer?.Dispose();
                _renewalTimer = null;
            }
        }

        private async Task ReleaseLockAsync()
        {
            if (string.IsNullOrEmpty(_lockToken))
            {
                _logger.LogWarning("No lock token available to release the lock.");
                return;
            }

            string lockKey = _lockName;

            try
            {
                var existingLock = await TryReadLockFile(lockKey);

                if (existingLock != null && existingLock.Value.Token == _lockToken)
                {
                    await _storageProvider.DeleteFile(lockKey);
                    _logger.LogInformation("Lock released.");
                    _lockToken = null;
                }
                else
                {
                    _logger.LogWarning("Cannot release lock not owned by this instance.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error releasing lock.");
            }
        }

        private async Task WriteLockFile(string lockKey, string lockToken, DateTimeOffset expiryTime)
        {
            string lockContent = $"{lockToken}\n{expiryTime:o}";
            byte[] lockBytes = System.Text.Encoding.UTF8.GetBytes(lockContent);
            await _storageProvider.UploadBytes(lockKey, lockBytes);
        }

        private async Task<(string Token, DateTimeOffset Expiry)?> TryReadLockFile(string lockKey)
        {
            try
            {
                var lockBytes = await _storageProvider.DownloadBytes(lockKey);
                string lockContent = System.Text.Encoding.UTF8.GetString(lockBytes.Span);
                var lines = lockContent.Split('\n');
                if (lines.Length >= 2 && DateTimeOffset.TryParse(lines[1], out var expiryTime))
                {
                    return (lines[0], expiryTime);
                }
            }
            catch (FileNotFoundException)
            {
                return null; // Lock file does not exist
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading lock file.");
            }

            return null; // Invalid lock file
        }
    }
}
