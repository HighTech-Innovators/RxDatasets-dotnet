using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace RxDatasets.DatasetUpdateNotifications;

public class DatasetUpdateNotifierAsync<T> : IDatasetUpdateNotifier<T>
{
    private readonly Subject<DatasetUpdateNotification> updatedDatasetSubject = new();

    public void Notify(DatasetUpdateNotification datasetUpdate)
    {
        updatedDatasetSubject.OnNext(datasetUpdate);
    }

    public IDisposable? Subscribe(Func<DatasetUpdateNotification, Task> handler)
    {
        return updatedDatasetSubject
            .ObserveOn(TaskPoolScheduler.Default)
            .Subscribe(async update => await handler(update));
    }
}
