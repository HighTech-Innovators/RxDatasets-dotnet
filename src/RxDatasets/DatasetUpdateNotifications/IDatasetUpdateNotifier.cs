using System.Reactive.Linq;

namespace RxDatasets.DatasetUpdateNotifications;

public interface IDatasetUpdateNotifier<T>
{
    void Notify(DatasetUpdateNotification datasetUpdate);
    IDisposable? Subscribe(Func<DatasetUpdateNotification, Task> handler);
}

public static class DatasetUpdateNotifierExtensions
{
    public static IObservable<DatasetUpdateNotification> AsObservable<T>(this IDatasetUpdateNotifier<T> notifier)
    {
        return Observable.Create<DatasetUpdateNotification>(observer =>
        {
            var subscription = notifier.Subscribe(async update =>
            {
                observer.OnNext(update);
            });

            return subscription;
        });
    }
}
