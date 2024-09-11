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
            if (!_subjects.TryGetValue(typeof(TNotification), out var subjectObj))
                return Task.CompletedTask;

            var subject = (ISubject<TNotification>)subjectObj;
            subject.OnNext(notification);

            return Task.CompletedTask;
        }

        private ISubject<TNotification> GetOrCreateSubject<TNotification>()
        {
            return (ISubject<TNotification>)_subjects.GetOrAdd(typeof(TNotification), _ => new Subject<TNotification>());
        }

        public void Dispose()
        {
            foreach (var subjectObj in _subjects.Values)
            {
                var subject = (ISubject<object>)subjectObj;
                subject.OnCompleted();
            }
            _subjects.Clear();
        }
    }
}
