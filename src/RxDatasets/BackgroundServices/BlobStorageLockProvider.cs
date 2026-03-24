using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.BackgroundServices;

public class BlobStorageLockProvider<TEntity>(
    IDiskStorage<TEntity> storage,
    ILogger<BlobStorageLockProvider<TEntity>> logger,
    IClock clock) : ILockProvider
{
    public async Task<(string Token, DateTimeOffset Expiry)?> AcquireLockAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken)
    {
        string lockToken = Guid.NewGuid().ToString();
        try
        {
            var existingLock = await TryReadLockFile(lockName);

            if (existingLock == null || existingLock.Value.Expiry < clock.UtcNow)
            {
                // No existing lock or the lock has expired, acquire a new lock
                DateTimeOffset expiryTime = clock.UtcNow.Add(duration);
                await WriteLockFile(lockName, lockToken, expiryTime);
                return (lockToken, expiryTime);
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

    public async Task<bool> RenewLockAsync(string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken)
    {
        try
        {
            var existingLock = await TryReadLockFile(lockName);

            if ((existingLock != null && existingLock.Value.Token == lockToken) || existingLock?.Expiry < clock.UtcNow)
            {
                DateTimeOffset expiryTime = clock.UtcNow.Add(duration);
                await WriteLockFile(lockName, lockToken, expiryTime);
                return true;
            }
            
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error renewing lock.");
            return false;
        }
    }

    public async Task ReleaseLockAsync(string lockName, string lockToken, CancellationToken cancellationToken)
    {
        try
        {
            var existingLock = await TryReadLockFile(lockName);

            if (existingLock != null && existingLock.Value.Token == lockToken)
            {
                await storage.DeleteFile(lockName);
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

    public async Task<bool> ShouldYieldAsync(string requestLockName, string instanceId, CancellationToken cancellationToken)
    {
        try
        {
            var bytes = await storage.DownloadBytes(requestLockName);
            if (bytes.IsEmpty) return false;

            var content = System.Text.Encoding.UTF8.GetString(bytes.Span).Trim();
            if (string.IsNullOrEmpty(content)) return false;

            return content != instanceId;
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

    public async Task RequestLockAsync(string requestLockName, string instanceId, CancellationToken cancellationToken)
    {
        await storage.UploadBytes(requestLockName, System.Text.Encoding.UTF8.GetBytes(instanceId));
    }

    public async Task DeleteRequestLockAsync(string requestLockName, CancellationToken cancellationToken)
    {
        try
        {
            await storage.DeleteFile(requestLockName);
        }
        catch (FileNotFoundException)
        {
            // Ignore
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

    public void Dispose()
    {
    }
}
