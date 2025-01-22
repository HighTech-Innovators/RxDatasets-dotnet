using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.BackgroundServices;

public class DatasetUpdateConsumingService<TDto, TConsumer> : BackgroundService
    where TConsumer : IDatasetUpdateConsumer<TDto>
{
    private volatile TaskCompletionSource _thereIsANotificationToProcess = new();
    private volatile int _iteration;
    private readonly ILogger<DatasetUpdateConsumingService<TDto, TConsumer>> _logger;
    private readonly INotificationsHub<TDto> _notificationsHub;
    private readonly TConsumer _datasetUpdateConsumer;
    private bool _storageProviderIsReady;

    public DatasetUpdateConsumingService(ILogger<DatasetUpdateConsumingService<TDto, TConsumer>> logger,
        INotificationsHub<TDto> notificationsHub,

        TConsumer datasetUpdateConsumer)
    {
        _logger = logger;
        _notificationsHub = notificationsHub;
        _datasetUpdateConsumer = datasetUpdateConsumer;
        _notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(OnStorageProviderReadinessNotification);
    }

    private Task OnStorageProviderReadinessNotification(StorageProviderReadinessNotifcation notification)
    {
        _storageProviderIsReady = notification.ServiceStatus == ServiceStatus.Ready;
        return Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"DatasetUpdateConsumingService<{_datasetUpdateConsumer.ConsumerIdentifier}> Started");
        try
        {
            // Start the loop before subscribing - especially important for UNIT TEST scenarios
            // where the subscription is Sync, and the message we immediately get there must be handled before the subscription is 'done'.
            var workAndWaitLoop = WorkAndWaitLoop(stoppingToken);

            using var subscription = _notificationsHub
                .Subscribe<DatasetUpdatedNotification>((_) => OnDatasetUpdated());

            await workAndWaitLoop;
        }
        finally
        {
            _logger.LogInformation($"DatasetUpdateConsumingService<{_datasetUpdateConsumer.ConsumerIdentifier}> Stopped");
        }
    }

    private async Task WorkAndWaitLoop(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!_storageProviderIsReady)
            {
                await Task.Delay(10, stoppingToken);
                continue;
            }
            
            try
            {
                // Process each consumer
                await _datasetUpdateConsumer.HandleNewDeltas(stoppingToken);
                Interlocked.Increment(ref _iteration);
                await _thereIsANotificationToProcess.Task.WaitAsync(stoppingToken);
                _thereIsANotificationToProcess = new TaskCompletionSource();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                _logger.LogWarning(e, "Exception during consuming notification");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task OnDatasetUpdated()
    {
        await QueueJobToProcess();
    }

    private async Task QueueJobToProcess()
    {
        var previousIteration = _iteration;
        _ = _thereIsANotificationToProcess.TrySetResult();
        while (_iteration == previousIteration)
            await Task.Delay(1);
    }
}