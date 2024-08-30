using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;

namespace RxDatasets.DatasetUpdateNotifications;

public class DatasetUpdateNotifierAsync<T> : IDatasetUpdateNotifier<T>
{
    private readonly Subject<DatasetUpdateNotification> _updatedDatasetSubject = new();

    public void Notify(DatasetUpdateNotification datasetUpdate)
    {
        _updatedDatasetSubject.OnNext(datasetUpdate);
    }

    public IDisposable? Subscribe(Func<DatasetUpdateNotification, Task> handler)
    {
        return _updatedDatasetSubject
            .ObserveOn(TaskPoolScheduler.Default)
            .Subscribe(async update => await handler(update).ToObservable());
    }
}