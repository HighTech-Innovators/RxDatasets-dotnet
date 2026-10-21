using System.Reactive.Concurrency;
using Microsoft.Reactive.Testing;
using RxDatasets.BackgroundServices;

namespace RxDatasets.AliasingTests;

/// <summary>
/// A test clock that wraps RxSchedulerClock and tracks pending delay requests.
/// This allows tests to wait until a delay is actually scheduled before advancing time.
/// </summary>
/// <remarks>
/// The code under test schedules its delays from its own threads, while the test advances time from the test
/// thread, and <see cref="TestScheduler"/> is not thread-safe: a delay scheduled while time is being advanced can
/// be lost, and the code under test then waits forever. Both go through one lock, so advance time with
/// <see cref="AdvanceBy"/> rather than on the scheduler directly.
/// </remarks>
internal sealed class TrackingRxSchedulerClock : IClock
{
    private readonly TestScheduler _scheduler;
    private readonly object _gate = new();
    private int _pendingDelays;

    public TrackingRxSchedulerClock(TestScheduler scheduler) => _scheduler = scheduler;

    public DateTimeOffset UtcNow
    {
        get
        {
            lock (_gate)
            {
                return _scheduler.Now;
            }
        }
    }

    /// <summary>
    /// Advances the scheduler's time, running the delays that come due.
    /// </summary>
    public void AdvanceBy(TimeSpan time)
    {
        lock (_gate)
        {
            _scheduler.AdvanceBy(time.Ticks);
        }
    }

    /// <summary>
    /// Returns the number of delays that have been scheduled but not yet completed.
    /// </summary>
    public int PendingDelays => Volatile.Read(ref _pendingDelays);

    public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _pendingDelays);

        var tcs = new TaskCompletionSource<object?>();
        IDisposable disp;
        lock (_gate)
        {
            disp = _scheduler.Schedule(dueTime, () =>
            {
                Interlocked.Decrement(ref _pendingDelays);
                tcs.TrySetResult(null);
            });
        }

        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.Register(() =>
            {
                Interlocked.Decrement(ref _pendingDelays);
                lock (_gate)
                {
                    disp.Dispose();
                }
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
