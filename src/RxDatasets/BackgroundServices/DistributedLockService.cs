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
    public class DistributedLockService<TEntity>(
        IDiskStorage<TEntity> storage,
        IOptions<StorageConfig> storageConfig,
        ILogger<DistributedLockService<TEntity>> logger,
        INotificationsHub<TEntity> notificationsHub)
        : IHostedService, IDisposable
    {
        private readonly TimeSpan _renewalInterval = TimeSpan.FromMinutes(5); // Interval to attempt lock renewal
        private Timer? _renewalTimer;
        private string? _lockToken;
        private bool _isDisposed;
        private readonly string _lockName = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value) + "lock-file";

        public bool HasLock => !string.IsNullOrEmpty(_lockToken);


        public Task StartAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Starting DistributedLockService...");
            Task.Run(async () =>
            {
                await AttemptAcquireLockAsync();
                var lastLogTime = DateTimeOffset.MinValue;

                while (!HasLock && !cancellationToken.IsCancellationRequested)
                {
                    // Check if it's time to log (only every 5 minutes)
                    if ((DateTimeOffset.UtcNow - lastLogTime).TotalMinutes >= 5)
                    {
                        logger.LogWarning("Lock is currently held by another process. Checking again in 5 seconds");
                        lastLogTime = DateTimeOffset.UtcNow;
                    }
            
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    await AttemptAcquireLockAsync();
                }
            }, cancellationToken);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Stopping DistributedLockService...");

            _renewalTimer?.Change(Timeout.Infinite, 0);
            logger.LogInformation("Application is stopping. Releasing lock...");

            try
            {
                // Ensure the lock is released before the application exits
                await ReleaseLockAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error releasing lock during application stopping.");
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

            logger.LogInformation("Attempting to acquire lock...");
            _lockToken = await AcquireLockAsync();

            if (HasLock)
            {
                logger.LogInformation("Lock acquired.");
                TimeSpan renewAfterTime = _renewalInterval - TimeSpan.FromSeconds(30);
                _renewalTimer = new Timer(async _ => await RenewLockAsync(), null, renewAfterTime, renewAfterTime);
            }
            else
            {
                logger.LogWarning("Failed to acquire lock.");
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
                logger.LogError(ex, "Error acquiring lock.");
                return null;
            }
        }

        private async Task RenewLockAsync()
        {
            if (!HasLock)
            {
                logger.LogWarning("No lock to renew.");
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
                    logger.LogInformation("Lock renewed.");
                }
                else
                {
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                    logger.LogWarning("Cannot renew lock not owned by this instance.");
                    _renewalTimer?.Change(Timeout.Infinite, 0);
                    _renewalTimer?.Dispose();
                    _renewalTimer = null;
                }
            }
            catch (Exception ex)
            {
                await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                logger.LogError(ex, "Error renewing lock.");
                _renewalTimer?.Change(Timeout.Infinite, 0);
                _renewalTimer?.Dispose();
                _renewalTimer = null;
            }
        }

        private async Task ReleaseLockAsync()
        {
            if (string.IsNullOrEmpty(_lockToken))
            {
                logger.LogWarning("No lock token available to release the lock.");
                return;
            }

            string lockKey = _lockName;

            try
            {
                var existingLock = await TryReadLockFile(lockKey);

                if (existingLock != null && existingLock.Value.Token == _lockToken)
                {
                    await storage.DeleteFile(lockKey);
                    logger.LogInformation("Lock released.");
                    _lockToken = null;
                }
                else
                {
                    logger.LogWarning("Cannot release lock not owned by this instance.");
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error releasing lock.");
            }
        }

        private async Task WriteLockFile(string lockKey, string lockToken, DateTimeOffset expiryTime)
        {
            string lockContent = $"{lockToken}\n{expiryTime:o}";
            byte[] lockBytes = System.Text.Encoding.UTF8.GetBytes(lockContent);
            await storage.UploadBytes(lockKey, lockBytes);
        }

        private async Task<(string Token, DateTimeOffset Expiry)?> TryReadLockFile(string lockKey)
        {
            try
            {
                var lockBytes = await storage.DownloadBytes(lockKey);
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
                logger.LogError(ex, "Error reading lock file.");
            }

            return null; // Invalid lock file
        }
    }
}
