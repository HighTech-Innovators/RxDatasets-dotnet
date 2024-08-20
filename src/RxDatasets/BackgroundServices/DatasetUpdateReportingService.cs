using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.DatasetUpdateNotifications;

namespace RxDatasets.BackgroundServices
{
    public class DatasetUpdateReportingService<T>(
        IDatasetUpdateNotifier<T> datasetUpdateNotifier,
        IStorageProvider<T> storageProvider,
        ILogger<DatasetUpdateReportingService<T>> logger,
        IDatasetUpdateReporting<T>? reportingImplementation = null)
        : IHostedService
    {
        private ulong _nextVersionToProcess = 0;
        private ulong _currentProcessedVersion = 0;
        private readonly string _serviceIdentifier = "ReporterService" + typeof(T).Name;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (reportingImplementation == null)
            {
                logger.LogInformation(typeof(T).Name + " reporting service did not start because no implementation is provided.");
                return;
            }
            logger.LogInformation(typeof(T).Name + " reporting service started");
            _currentProcessedVersion = await storageProvider.GetProcessedVersion(_serviceIdentifier);
            _nextVersionToProcess = _currentProcessedVersion + 1;
            _ = ProcessUnreportedChanges();
            datasetUpdateNotifier.Subscribe(ReportAsync);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public async Task ProcessUnreportedChanges()
        {
            try
            {
                ulong newestDelta = (await storageProvider.ListVersions()).newestDelta;
                while (_currentProcessedVersion < newestDelta && newestDelta > 0)
                {
                    await ProcessDelta(_nextVersionToProcess);
                }
            }
            catch (Exception e)
            {
                logger.LogWarning($"something went wrong while reporting {e.Message}", e);
            }
        }

        public async Task ReportAsync(DatasetUpdateNotification datasetUpdateNotification)
        {
            try
            {
                if (datasetUpdateNotification.Version != _currentProcessedVersion)
                {
                    await ProcessDelta(datasetUpdateNotification.Version);
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning($"something went wrong while reporting {e.Message}", e);
            }
        }

        private async Task ProcessDelta(ulong deltaVersion)
        {
            var optionalState = await reportingImplementation?.OpenDeltaTransaction(deltaVersion)!;
            try
            {
                var deltaIndex = await storageProvider.ReadDeltaIndex(deltaVersion);
                var deltaIndexParsed = DeltaIndex.Parser.ParseFrom(deltaIndex.Span);
                foreach (var newEntity in deltaIndexParsed.NewEntities)
                {
                    await reportingImplementation!.OnNewDto(newEntity.Name,
                        (await storageProvider.ReadDeltaData(deltaVersion, newEntity.Position, newEntity.Length)).ToArray(),
                        optionalState);
                }

                foreach (var updatedEntity in deltaIndexParsed.ChangedEntities)
                {
                    await reportingImplementation!.OnChangedDto(updatedEntity.Name,
                        (await storageProvider.ReadDeltaData(deltaVersion, updatedEntity.Position,
                            updatedEntity.Length)).ToArray(),
                        optionalState);
                }

                foreach (var deletedEntity in deltaIndexParsed.DeletedNames)
                {
                    await reportingImplementation!.OnDeletedDto(deletedEntity,
                        optionalState);
                }

                if (optionalState != null) await reportingImplementation.CommitVersionTransaction(optionalState);
                optionalState = null;
                await storageProvider.StoreProcessedVersion(_serviceIdentifier, deltaVersion);
                _currentProcessedVersion = deltaVersion;
                _nextVersionToProcess = _currentProcessedVersion + 1;
            }
            catch (FileNotFoundException)
            {
                logger.LogWarning(
                    $"Index/Data files for version {deltaVersion} don't exist. Either data is corrupted or is deleted.");
                _currentProcessedVersion = deltaVersion;
                _nextVersionToProcess++; //we do this to skip the file and go to the next.
            }
            finally
            {
                if (optionalState != null)
                {
                    await reportingImplementation.RollBack(optionalState);
                }
            }
        }

    }
}