using System.Reactive.Concurrency;
using RxDatasets.BackgroundServices;

namespace RxDatasets.AliasingTests;

/// <summary>
/// A test clock that wraps RxSchedulerClock and tracks pending delay requests.
/// This allows tests to wait until a delay is actually scheduled before advancing time.
/// </summary>
internal sealed class TrackingRxSchedulerClock : IClock
{
    private readonly IScheduler _scheduler;
    private int _pendingDelays;

    public TrackingRxSchedulerClock(IScheduler scheduler) => _scheduler = scheduler;

    public DateTimeOffset UtcNow => _scheduler.Now;

    /// <summary>
    /// Returns the number of delays that have been scheduled but not yet completed.
    /// </summary>
    public int PendingDelays => Volatile.Read(ref _pendingDelays);

    public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _pendingDelays);

        var tcs = new TaskCompletionSource<object?>();
        var disp = _scheduler.Schedule(dueTime, () =>
        {
            Interlocked.Decrement(ref _pendingDelays);
            tcs.TrySetResult(null);
        });

        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.Register(() =>
            {
                Interlocked.Decrement(ref _pendingDelays);
                disp.Dispose();
                tcs.TrySetCanceled(cancellationToken);
            });
        }

        return tcs.Task;
    }

    /// <summary>
    /// Waits until at least one delay is pending (scheduled but not yet completed).
    /// Throws TimeoutException if no delay is scheduled within the timeout.
    /// </summary>
    public async Task WaitUntilAnyDelaysPending(TimeSpan timeout, string because)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (PendingDelays == 0)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"No pending delays within {timeout}: {because}");
            }
            await Task.Delay(1);
        }
    }
}
