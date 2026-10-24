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
    // Runs of the consumer that have started, and the number of the last one that completed. A notification waits for
    // a run that started after it: a run already in progress may have read the dataset before the delta was stored.
    private long _startedRuns;
    private long _completedRun;
    // Completed, and replaced, after every completed run: what a notification waiting for its run waits on
    private volatile TaskCompletionSource _runCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ILogger<DatasetUpdateConsumingService<TDto, TConsumer>> _logger;
    private readonly INotificationsHub<TDto> _notificationsHub;
    private readonly TConsumer _datasetUpdateConsumer;
    private bool _storageProviderIsReady;
    // Cancelled when the service stops; ends notifications still waiting for their run
    private readonly CancellationTokenSource _stopping = new();
    private IDisposable? _datasetUpdatedSubscription;

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
        _storageProviderIsReady = notification is { ServiceStatus: ServiceStatus.Ready, IsLeader: true };
        return Task.CompletedTask;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Starts the loop in ExecuteAsync
        await base.StartAsync(cancellationToken);

        // Subscribed here, not in ExecuteAsync: since .NET 10, BackgroundService runs ExecuteAsync on the thread pool
        // after StartAsync returns, so a delta notified in between would not wait for the consumer. A notification
        // sent before the start is replayed by a synchronous notifications hub, and waits for the loop's run.
        _datasetUpdatedSubscription = _notificationsHub
            .Subscribe<DatasetUpdatedNotification>(_ => OnDatasetUpdated(_stopping.Token));
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();
        _datasetUpdatedSubscription?.Dispose();
        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        // Cancelled, not disposed, as BackgroundService does with its own token source: the host can still call
        // StopAsync after disposing the service
        _stopping.Cancel();
        _datasetUpdatedSubscription?.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"DatasetUpdateConsumingService<{_datasetUpdateConsumer.ConsumerIdentifier}> Started");
        try
        {
            await WorkAndWaitLoop(stoppingToken);
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
                var run = Interlocked.Increment(ref _startedRuns);
                await _datasetUpdateConsumer.HandleNewDeltas(stoppingToken);
                Interlocked.Exchange(ref _completedRun, run);
                Interlocked.Exchange(ref _runCompleted, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
                    .SetResult();
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

    /// <summary>
    /// Wakes the loop for a new delta, and returns once a run has handled it: with a synchronous notifications hub,
    /// the write that sent the notification then returns only after the consumer has seen it.
    /// </summary>
    private async Task OnDatasetUpdated(CancellationToken stoppingToken)
    {
        // The delta was stored before it was notified, so the first run that starts after this read sees it
        var awaitedRun = Interlocked.Read(ref _startedRuns) + 1;
        _ = _thereIsANotificationToProcess.TrySetResult();
        // No run is coming on a host that isn't ready, or doesn't run the background work: don't hold up the writer.
        // Once it is, its first run handles every delta stored since.
        if (!_storageProviderIsReady)
            return;

        while (true)
        {
            // Take the signal before checking: the loop records the run before it completes the signal, so if the
            // run isn't recorded yet, this is the signal it completes
            var runCompleted = _runCompleted.Task;
            if (Interlocked.Read(ref _completedRun) >= awaitedRun)
                return;

            try
            {
                await runCompleted.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // The service is stopping: no run will handle this delta any more. Returning rather than throwing
                // keeps the notification itself working, for the writer that sent it and for other subscribers.
                return;
            }
        }
    }
}
