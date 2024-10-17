using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Reactive.Disposables;

namespace RxDatasets.GenericNotifierLogic
{
    public class NotificationsHub<T> : INotificationsHub<T>
    {
        private readonly ConcurrentDictionary<Type, ConcurrentBag<object>> _channels = new();
        private readonly ConcurrentDictionary<Type, object> _lastNotifications = new();

        public IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification
        {
            var channel = CreateChannel<TNotification>(); // Create a new channel per subscription

            // Replay the last notification if it exists
            if (_lastNotifications.TryGetValue(typeof(TNotification), out var lastNotificationObj))
            {
                var lastNotification = (TNotification)lastNotificationObj;
                channel.Writer.TryWrite(lastNotification);
            }

            var cancellationDisposable = new CancellationDisposable();
            var cancellationToken = cancellationDisposable.Token;

            // Start a listener task that processes items from the channel asynchronously
            Task.Run(async () =>
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

            return cancellationDisposable;
        }

        public async Task Notify<TNotification>(TNotification notification) where TNotification : INotification
        {
            // Store the last notification
            _lastNotifications[typeof(TNotification)] = notification;

            if (!_channels.TryGetValue(typeof(TNotification), out var channels))
                return;

            foreach (var channel in channels)
            {
                await ((Channel<TNotification>)channel).Writer.WriteAsync(notification);
            }
        }

        private Channel<TNotification> CreateChannel<TNotification>()
        {
            var channel = Channel.CreateUnbounded<TNotification>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

            var typeChannels = _channels.GetOrAdd(typeof(TNotification), _ => new ConcurrentBag<object>());
            typeChannels.Add(channel);
            return channel;
        }
    }
}
