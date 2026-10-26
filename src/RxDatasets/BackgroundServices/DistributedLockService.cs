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
        IDiskStorage<TEntity> diskStorage,
        IOptions<StorageConfig> storageConfig,
        ILogger<DistributedLockService<TEntity>> logger,
        INotificationsHub<TEntity> notificationsHub,
        IOptions<DistributedLockOptions>? lockOptions = null,
        IClock? clock = null)
        : IHostedService, IDisposable
    {
        // ponytail: no fallback to plain read-then-write; without compare-and-swap two instances can both hold the lock.
        // This only guards the lock file. A holder whose lock expired unnoticed is stopped from overwriting deltas by the
        // fenced delta commit (ObjectStorageProvider), not from writing snapshots, SnapshotInfo.pb, ProcessedVersions.pb
        // or cleaning.
        private readonly IConditionalDiskStorage<TEntity> storage = diskStorage as IConditionalDiskStorage<TEntity>
            ?? throw new NotSupportedException($"{diskStorage.GetType().Name} does not implement "
                + $"{nameof(IConditionalDiskStorage<TEntity>)}, which the distributed lock requires");
        private readonly TimeSpan _renewalInterval = lockOptions?.Value.RenewalInterval ?? TimeSpan.FromMinutes(5);
        private readonly TimeSpan _pollingInterval = lockOptions?.Value.PollingInterval ?? TimeSpan.FromSeconds(5);
        private readonly TimeSpan _renewalThreshold = lockOptions?.Value.RenewalThreshold ?? TimeSpan.FromSeconds(30);
        private readonly TimeSpan _handoverTimeout = ValidateHandoverTimeout(lockOptions?.Value.HandoverTimeout ?? TimeSpan.FromSeconds(15), lockOptions?.Value.PollingInterval ?? TimeSpan.FromSeconds(5));
        private readonly IClock _clock = clock ?? new SystemClock();
        private string? _lockToken;
        private string? _attemptedToken;
        private string? _lockVersion;
        private DateTimeOffset _lockExpiry;
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

            if (!HasLock)
            {
                // Normal when another instance holds the lock or this instance yielded it during a handover
                logger.LogInformation("Lock not held by this instance (state: {State}); nothing to release.", _state);
                return;
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
            string lockToken = Guid.NewGuid().ToString();

            try
            {
                var existingLock = await TryReadLockFile();

                // An earlier attempt of ours landed without us seeing the response: that lock is ours
                if (existingLock != null && existingLock.Value.Token == _attemptedToken
                    && existingLock.Value.Expiry >= _clock.UtcNow)
                {
                    _lockVersion = existingLock.Value.Version;
                    _lockExpiry = existingLock.Value.Expiry;
                    await notificationsHub.Notify<DistributedLockNotification>(new(DistirbutedLockStatus.Acquried));
                    return _attemptedToken;
                }

                if (existingLock == null || existingLock.Value.Expiry < _clock.UtcNow)
                {
                    // No existing lock or the lock has expired: take it, unless another instance changed it since we read it
                    DateTimeOffset expiryTime = _clock.UtcNow.Add(_renewalInterval);
                    _attemptedToken = lockToken;
                    string? version = await storage.TryUploadBytes(
                        _lockName, LockBytes(lockToken, expiryTime), existingLock?.Version);
                    if (version == null)
                    {
                        logger.LogInformation("Lock was taken by another instance while acquiring.");
                        return null;
                    }

                    _lockVersion = version;
                    _lockExpiry = expiryTime;
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

            try
            {
                DateTimeOffset expiryTime = _clock.UtcNow.Add(_renewalInterval);
                if (await TryWriteOwnLockAsync(expiryTime))
                {
                    _lockExpiry = expiryTime;
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
                if (_lockExpiry < _clock.UtcNow.Add(_pollingInterval))
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

            try
            {
                // Release by expiring the lock instead of deleting it: not every provider supports a conditional delete
                if (await TryWriteOwnLockAsync(DateTimeOffset.MinValue))
                {
                    logger.LogInformation("Lock released.");
                }
                else
                {
                    logger.LogWarning("Cannot release lock not owned by this instance.");
                }
                _lockToken = null;
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

        /// <summary>
        /// Writes our lock over the version we last wrote. Returns false if another instance owns the lock now.
        /// </summary>
        private async Task<bool> TryWriteOwnLockAsync(DateTimeOffset expiryTime)
        {
            byte[] lockBytes = LockBytes(_lockToken!, expiryTime);
            string? version = await storage.TryUploadBytes(_lockName, lockBytes, _lockVersion);
            if (version == null)
            {
                // An earlier write of ours may have landed without us seeing the response; only we write our token
                var current = await TryReadLockFile();
                if (current != null && current.Value.Token == _lockToken)
                {
                    version = await storage.TryUploadBytes(_lockName, lockBytes, current.Value.Version);
                }
            }

            if (version == null)
            {
                return false;
            }

            _lockVersion = version;
            return true;
        }

        private static byte[] LockBytes(string lockToken, DateTimeOffset expiryTime) =>
            System.Text.Encoding.UTF8.GetBytes($"{lockToken}\n{expiryTime:o}");

        /// <summary>
        /// Returns null if there is no lock file; an unparsable lock file counts as expired. Other errors are thrown.
        /// </summary>
        private async Task<(string Token, DateTimeOffset Expiry, string Version)?> TryReadLockFile()
        {
            try
            {
                var (lockBytes, version) = await storage.DownloadBytesWithVersion(_lockName);
                string lockContent = System.Text.Encoding.UTF8.GetString(lockBytes.Span);
                var lines = lockContent.Split('\n');
                if (lines.Length >= 2 && DateTimeOffset.TryParse(lines[1], out var expiryTime))
                {
                    return (lines[0], expiryTime, version);
                }

                logger.LogWarning("Invalid lock file, treating it as expired.");
                return (string.Empty, DateTimeOffset.MinValue, version);
            }
            catch (FileNotFoundException)
            {
                return null; // Lock file does not exist
            }
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
