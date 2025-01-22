using System.Collections.Concurrent;
using System.Reactive.Subjects;

namespace RxDatasets.GenericNotifierLogic
{
    public class NotificationsHubSync<T> : INotificationsHub<T>, IDisposable
    {
        private readonly ConcurrentDictionary<Type, object> _subjects = new();

        public IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification
        {
            var subject = GetOrCreateSubject<TNotification>();
            var subscription = subject.Subscribe(update => handler(update).Wait());
            return subscription;
        }

        public Task Notify<TNotification>(TNotification notification) where TNotification : INotification
        {
            var subject = GetOrCreateSubject<TNotification>();
            
            subject.OnNext(notification);

            return Task.CompletedTask;
        }

        private ReplaySubject<TNotification> GetOrCreateSubject<TNotification>()
        {
            return (ReplaySubject<TNotification>)_subjects.GetOrAdd(typeof(TNotification), _ => new ReplaySubject<TNotification>(1));
        }

        public void Dispose()
        {
            foreach (var subjectObj in _subjects.Values)
            {
                if (subjectObj is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _subjects.Clear();
        }
    }
}
