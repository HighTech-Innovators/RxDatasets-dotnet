using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Reactive.Disposables;

namespace RxDatasets.GenericNotifierLogic
{
    public class NotificationsHub<T> : INotificationsHub<T>, IDisposable
    {
        private readonly ConcurrentDictionary<Type, ConcurrentBag<object>> _channels = new();
        private readonly ConcurrentBag<Task> _listenerTasks = []; // Store all listener tasks

        public IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification 
        {
            var channel = CreateChannel<TNotification>(); // Create a new channel per subscription
            var cancellationDisposable = new CancellationDisposable();
            var cancellationToken = cancellationDisposable.Token;

            // Start a listener task that processes items from the channel asynchronously
            var listenerTask = Task.Run(async () =>
            {
                await foreach (var notification in channel.Reader.ReadAllAsync(cancellationToken))
                {
                    try
                    {
                        await handler(notification);
                    }
                    catch (Exception ex)
                    {
                        // Ignored, handlers should handle failure cases
                    }
                }
            }, cancellationToken);

            // Store the task and disposable for cleanup later
            _listenerTasks.Add(listenerTask);

            return cancellationDisposable;
        }

        public async Task Notify<TNotification>(TNotification notification) where TNotification : INotification
        {
            if (!_channels.TryGetValue(typeof(TNotification), out _))
                return;
            
            foreach (var channel in _channels[typeof(TNotification)])
            { 
                await ((Channel<TNotification>)channel).Writer.WriteAsync(notification);
            }
        }

        private Channel<TNotification> CreateChannel<TNotification>()
        {
            var channel = Channel.CreateUnbounded<TNotification>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });

            var typeChannels = _channels.GetOrAdd(typeof(TNotification), _ => []);
            typeChannels.Add(channel);
            return channel;
        }

        public void Dispose()
        {
            foreach (var listenerTask in _listenerTasks)
            {
                listenerTask.Dispose();
            }

            _channels.Clear();
        }
    }
}
