using System;
using System.Threading;
using System.Threading.Tasks;

namespace RxDatasets.BackgroundServices
{
    public sealed class SystemClock : IClock
    {
        public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
        public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default)
            => Task.Delay(dueTime, cancellationToken);
    }
}
