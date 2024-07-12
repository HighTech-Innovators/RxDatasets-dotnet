using System.Reactive.Subjects;

namespace HTI.RxDatasets.DatasetUpdateNotifications;

public class DatasetUpdateNotifierSync<T> : IDatasetUpdateNotifier<T>
{
    private readonly Subject<DatasetUpdateNotification> updatedDatasetSubject = new();

    public void Notify(DatasetUpdateNotification datasetUpdate)
    {
        updatedDatasetSubject.OnNext(datasetUpdate);
    }

    public IDisposable? Subscribe(Func<DatasetUpdateNotification, Task> handler)
    {
        return updatedDatasetSubject.Subscribe(update => handler(update).Wait());
    }
}
