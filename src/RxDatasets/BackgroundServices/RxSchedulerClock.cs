using System;
using System.Reactive.Concurrency;
using System.Threading;
using System.Threading.Tasks;

namespace RxDatasets.BackgroundServices
{
    public sealed class RxSchedulerClock : IClock
    {
        private readonly IScheduler _scheduler;
        public RxSchedulerClock(IScheduler scheduler) => _scheduler = scheduler;

        public DateTimeOffset UtcNow => _scheduler.Now;

        public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default)
        {
            var tcs = new TaskCompletionSource<object?>(
                //TaskCreationOptions.RunContinuationsAsynchronously
                );
            var disp = _scheduler.Schedule(dueTime, () => tcs.TrySetResult(null));
            if (cancellationToken.CanBeCanceled)
            {
                cancellationToken.Register(() =>
                {
                    disp.Dispose();
                    tcs.TrySetCanceled(cancellationToken);
                });
            }
            return tcs.Task;
        }
    }
}
