namespace RxDatasets;

/// <summary>
/// Takes a <see cref="SemaphoreSlim"/> used as a lock, returning a disposable that releases it again:
/// <c>using (await semaphore.LockAsync(ct)) { ... }</c>.
/// </summary>
public static class SemaphoreSlimExtensions
{
    /// <summary>
    /// Waits for the semaphore and returns a disposable that releases it, once.
    /// </summary>
    public static async Task<IDisposable> LockAsync(this SemaphoreSlim semaphore, CancellationToken cancellationToken = default)
    {
        await semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => semaphore.Release());
    }

    /// <summary>
    /// Synchronous counterpart of <see cref="LockAsync"/>, for methods that cannot await. Blocking on
    /// <c>LockAsync().Result</c> instead can deadlock on a single-threaded synchronization context.
    /// </summary>
    public static IDisposable Lock(this SemaphoreSlim semaphore, CancellationToken cancellationToken = default)
    {
        semaphore.Wait(cancellationToken);
        return new ActionDisposable(() => semaphore.Release());
    }
}
