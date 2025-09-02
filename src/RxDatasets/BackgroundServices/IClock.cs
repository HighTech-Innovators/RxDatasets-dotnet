using System;
using System.Threading;
using System.Threading.Tasks;

namespace RxDatasets.BackgroundServices
{
    public interface IClock
    {
        DateTimeOffset UtcNow { get; }
        Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default);
    }
}
