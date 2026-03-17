using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.BackgroundServices
{
    public class DistributedLockService<TEntity>(
        IDiskStorage<TEntity> storage,
        IOptions<StorageConfig> storageConfig,
        ILogger<DistributedLockService<TEntity>> logger,
        INotificationsHub<TEntity> notificationsHub,
        IOptions<DistributedLockOptions>? lockOptions = null,
        IClock? clock = null)
        : IHostedService, IDisposable
    {
        private readonly TimeSpan _renewalInterval = lockOptions?.Value.RenewalInterval ?? TimeSpan.FromMinutes(5);
        private readonly TimeSpan _pollingInterval = lockOptions?.Value.PollingInterval ?? TimeSpan.FromSeconds(5);
        private readonly TimeSpan _renewalThreshold = lockOptions?.Value.RenewalThreshold ?? TimeSpan.FromSeconds(30);
        private readonly TimeSpan _handoverTimeout = ValidateHandoverTimeout(lockOptions?.Value.HandoverTimeout ?? TimeSpan.FromSeconds(15), lockOptions?.Value.PollingInterval ?? TimeSpan.FromSeconds(5));
        private readonly IClock _clock = clock ?? new SystemClock();
        private string? _lockToken;
        private bool _isDisposed;
        private DateTimeOffset _lastRenewalTime;
        private readonly CancellationTokenSource _cts = new();
        private Task? _loopTask;
        private readonly string _lockName = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value) + "lock-file";
        private readonly string _requestLockName = DatasetHelper.GetObjectNamePrefix<TEntity>(storageConfig.Value) + "request-lock";
        private readonly string _instanceId = Guid.NewGuid().ToString();
        private LockServiceState _state = LockServiceState.Idle;
        private DateTimeOffset _lastYieldTime;

        public bool HasLock => !string.IsNullOrEmpty(_lockToken);
        public LockServiceState State => _state;

        private static TimeSpan ValidateHandoverTimeout(TimeSpan handover, TimeSpan polling)
        {
            if (handover <= polling)
                throw new InvalidOperationException($"HandoverTimeout ({handover}) must be greater than PollingInterval ({polling})");
            return handover;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Starting DistributedLockService...");
            _loopTask = Task.Run(async () =>
            {
                try
                {
                    logger.LogInformation("Writing request-lock file with InstanceId: {InstanceId}", _instanceId);
                    await storage.UploadBytes(_requestLockName, System.Text.Encoding.UTF8.GetBytes(_instanceId));
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to write request-lock file on startup");
                }

                var lastLogTime = DateTimeOffset.MinValue;

                while (!_cts.Token.IsCancellationRequested)
                {
                    //Console.WriteLine($"Poll @ {_clock.UtcNow}");
                    if (!HasLock)
                    {
                        if (_state != LockServiceState.Yielded)
                        {
                            await AttemptAcquireLockAsync();
                        }
                        else if (_clock.UtcNow - _lastYieldTime >= _handoverTimeout)
                        {
                            ChangeState(LockServiceState.Idle, "Handover timed out, reclaiming lock");
                            await AttemptAcquireLockAsync();
                        }

                        if (!HasLock)
                        {
                            // Check if it's time to log (only every 5 minutes)
                            if ((_clock.UtcNow - lastLogTime).TotalMinutes >= 5)
                            {
                                logger.LogWarning("Lock is currently held by another process. Checking again in 5 seconds");
                                lastLogTime = _clock.UtcNow;
                            }
                        }
                    }
                    else
                    {
                        if (await ShouldYieldAsync())
                        {
                            await ReleaseLockAsync();
                            if (!HasLock)
                            {
                                await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                            }
                            ChangeState(LockServiceState.Yielded, "Yielding lock due to request from another instance");
                            _lastYieldTime = _clock.UtcNow;
                        }
                        else if (_clock.UtcNow - _lastRenewalTime >= _renewalInterval - _renewalThreshold)
                        {
                            await RenewLockAsync();
                        }
                    }
                    // Only catching TaskCanceledException from the delay operation to allow graceful shutdown.
                    try
                    {
                        await _clock.Delay(_pollingInterval, _cts.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        // Task was canceled, which is expected during shutdown; just exit the loop
                    }
                }
            }, cancellationToken);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            logger.LogInformation("Stopping DistributedLockService...");

            if (!_isDisposed)
            {
                try
                {
                    _cts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // CancellationTokenSource was already disposed
                }
            }

            if (_loopTask != null)
            {
                await _loopTask;
            }

            try
            {
                // Clean up request file if it matches our instance ID
                var bytes = await storage.DownloadBytes(_requestLockName);
                string content = System.Text.Encoding.UTF8.GetString(bytes.Span);
                if (content == _instanceId)
                {
                    await storage.DeleteFile(_requestLockName);
                }
            }
            catch { /* Ignore if file doesn't exist or error */ }

            try
            {
                // Flush any queued writes before releasing the lock, so the next lock holder
                // sees up-to-date metadata (DeltaInfo, ProcessedVersions, etc.)
                logger.LogInformation("Flushing storage write queue before releasing lock...");
                await storage.FlushAsync();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error flushing storage write queue during shutdown.");
            }

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

            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Already disposed
            }

            _cts.Dispose();
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
                ChangeState(LockServiceState.Active, "Lock acquired");
                _lastRenewalTime = _clock.UtcNow;

                try
                {
                    await storage.DeleteFile(_requestLockName);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to delete request-lock file after acquisition.");
                }
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

                if (existingLock == null || existingLock.Value.Expiry < _clock.UtcNow)
                {
                    // No existing lock or the lock has expired, acquire a new lock
                    DateTimeOffset expiryTime = _clock.UtcNow.Add(_renewalInterval);
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
            (string Token, DateTimeOffset Expiry)? existingLock = null;

            try
            {
                existingLock = await TryReadLockFile(lockKey);

                if ((existingLock != null && existingLock.Value.Token == _lockToken) || existingLock?.Expiry < _clock.UtcNow)
                {
                    DateTimeOffset expiryTime = _clock.UtcNow.Add(_renewalInterval);
                    await WriteLockFile(lockKey, _lockToken!, expiryTime);
                    logger.LogInformation("Lock renewed.");
                    _lastRenewalTime = _clock.UtcNow;
                }
                else
                {
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                    logger.LogWarning("Cannot renew lock not owned by this instance.");
                    _lockToken = null;
                }
            }
            catch (Exception ex)
            {
                if (existingLock != null && existingLock.Value.Token == _lockToken && existingLock.Value.Expiry < _clock.UtcNow.Add(_pollingInterval))
                {
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Lost));
                    logger.LogError(ex, "Error renewing lock, and lock has expired. Notifying lost.");
                    _lockToken = null;
                }
                else
                {
                    logger.LogError(ex, "Error renewing lock. Will try again later");
                }
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

        private void ChangeState(LockServiceState newState, string reason)
        {
            if (_state != newState)
            {
                logger.LogInformation("State transition: {OldState} -> {NewState}. Reason: {Reason}", _state, newState, reason);
                _state = newState;
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

        private async Task<bool> ShouldYieldAsync()
        {
            try
            {
                var bytes = await storage.DownloadBytes(_requestLockName);
                if (bytes.IsEmpty) return false;

                var content = System.Text.Encoding.UTF8.GetString(bytes.Span).Trim();
                if (string.IsNullOrEmpty(content)) return false;

                return content != _instanceId;
            }
            catch (FileNotFoundException)
            {
                return false;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error checking for lock request.");
                return false;
            }
        }
    }
}
