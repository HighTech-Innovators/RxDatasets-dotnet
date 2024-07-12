namespace HTI.RxDatasets.DatasetUpdateNotifications;

public interface IDatasetUpdateNotifier<T>
{
    void Notify(DatasetUpdateNotification datasetUpdate);
    IDisposable? Subscribe(Func<DatasetUpdateNotification, Task> handler);
}