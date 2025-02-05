namespace RxDatasets.GenericNotifierLogic;

// ReSharper disable once UnusedTypeParameter
public interface INotificationsHub<TEntity>
{
    IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification;
    Task Notify<TNotification>(TNotification notification) where TNotification : INotification;
}