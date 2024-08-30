using System.Reactive.Linq;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.DatasetUpdateNotifications;

namespace RxDatasets.BackgroundServices;

public class DatasetUpdateConsumingService<TDto, TConsumer>(
    ILogger<DatasetUpdateConsumingService<TDto, TConsumer>> logger,
    IDatasetUpdateNotifier<TDto> datasetUpdateNotifier,
    TConsumer datasetUpdateConsumer) : BackgroundService
    where TConsumer : IDatasetUpdateConsumer<TDto>
{
    private volatile TaskCompletionSource _thereIsANotificationToProcess = new();
    private volatile int _iteration;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation($"DatasetUpdateConsumingService<{datasetUpdateConsumer.ConsumerIdentifier}> Started");
        using var subscription = datasetUpdateNotifier
            .AsObservable()
            .Select(not => not.Version)
            .DistinctUntilChanged()
            .Subscribe(async (_)=> await OnDatasetUpdated());

        try
        {
            await WorkAndWaitLoop(stoppingToken);
        }
        finally
        {
            logger.LogInformation($"DatasetUpdateConsumingService<{datasetUpdateConsumer.ConsumerIdentifier}> Stopped");
        }
    }

    private async Task WorkAndWaitLoop(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Process each consumer
                await datasetUpdateConsumer.HandleNewDeltas(stoppingToken);
                Interlocked.Increment(ref _iteration);
                await _thereIsANotificationToProcess.Task.WaitAsync(stoppingToken);
                _thereIsANotificationToProcess = new TaskCompletionSource();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning(e, "Exception during consuming notification");
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