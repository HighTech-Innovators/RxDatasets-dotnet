namespace RxDatasets.GenericNotifierLogic;

public interface INotificationsHub<TEntity>
{
    IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification;
    Task Notify<TNotification>(TNotification notification) where TNotification : INotification;
}