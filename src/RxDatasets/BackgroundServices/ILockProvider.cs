using RxDatasets.Abstractions;

namespace RxDatasets.BackgroundServices;

public interface ILockProvider : IDisposable
{
    Task<(string Token, DateTimeOffset Expiry)?> AcquireLockAsync(string lockName, TimeSpan duration, CancellationToken cancellationToken);
    Task<bool> RenewLockAsync(string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken);
    Task ReleaseLockAsync(string lockName, string lockToken, CancellationToken cancellationToken);
    Task<bool> ShouldYieldAsync(string requestLockName, string instanceId, CancellationToken cancellationToken);
    Task RequestLockAsync(string requestLockName, string instanceId, CancellationToken cancellationToken);
    Task DeleteRequestLockAsync(string requestLockName, CancellationToken cancellationToken);
}
