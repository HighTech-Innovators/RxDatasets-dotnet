using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.BackgroundServices
{
    public class DatasetUpdateReportingPlugin<TDto, TReporter>(
        IStorageProvider<TDto> storageProvider,
        ILogger<DatasetUpdateReportingPlugin<TDto, TReporter>> logger,
        TReporter reportingImplementation)
        : IDatasetUpdateConsumer<TDto> where TReporter : IDatasetUpdateReporting<TDto>
    {
        private readonly string _reportingIdentifier = $"{reportingImplementation.ReportingIdentifier}";
        
        public async Task ProcessAllUnreportedDeltas(CancellationToken stoppingToken)
        {
            var newestDelta = (await storageProvider.ListVersions()).newestDelta;
            var lastProcessedDelta = await storageProvider.GetProcessedVersion(_reportingIdentifier);

            while (!stoppingToken.IsCancellationRequested && lastProcessedDelta < newestDelta)
            {
                try
                {
                    var nextVersionToProcess = lastProcessedDelta + 1;
                    await ProcessDelta(nextVersionToProcess, stoppingToken);
                    lastProcessedDelta++;
                    await storageProvider.StoreProcessedVersion(_reportingIdentifier, lastProcessedDelta);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    logger.LogWarning($"something went wrong while reporting {e.Message}", e);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
        }

        private async Task ProcessDelta(ulong deltaVersion, CancellationToken cancellationToken)
        {
            DeltaIndex? deltaIndexParsed;
            try
            {
                var deltaIndex = await storageProvider.ReadDeltaIndex(deltaVersion);
                deltaIndexParsed = DeltaIndex.Parser.ParseFrom(deltaIndex.Span);
            }
            catch (FileNotFoundException)
            {
                logger.LogWarning(
                    $"Index file for version {deltaVersion} doesn't exist. Either data is corrupted or is deleted.");
                return;
            }

            var optionalState = await reportingImplementation?.OpenDeltaTransaction(deltaVersion)!;
            try
            {
                foreach (var newEntity in deltaIndexParsed.NewEntities)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.DELTA, deltaVersion, newEntity.Position)], newEntity.Length);
                    await reportingImplementation!.OnNewDto(newEntity.Name, data.ToArray(),
                        optionalState);
                }

                foreach (var updatedEntity in deltaIndexParsed.ChangedEntities)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.DELTA, deltaVersion, updatedEntity.Position)], updatedEntity.Length);

                    await reportingImplementation!.OnChangedDto(updatedEntity.Name, data.ToArray(),
                        optionalState);
                }

                foreach (var deletedEntity in deltaIndexParsed.DeletedNames)
                {
                    
                    cancellationToken.ThrowIfCancellationRequested();
                    await reportingImplementation!.OnDeletedDto(deletedEntity,
                        optionalState);
                }

                if (optionalState != null) await reportingImplementation.CommitDeltaTransaction(optionalState);
                optionalState = null;
            }
            catch (FileNotFoundException)
            {
                logger.LogWarning(
                    $"Data files for version {deltaVersion} don't exist. Either data is corrupted or is deleted.");
            }
            finally
            {
                if (optionalState != null)
                {
                    await reportingImplementation.RollBackDeltaTransaction(optionalState);
                }
            }
        }

        public async Task HandleNewDeltas(CancellationToken stoppingToken)
        {
            await ProcessAllUnreportedDeltas(stoppingToken);
        }

        public string ConsumerIdentifier { get; } = $"DatasetUpdatePlugin-of-{reportingImplementation.ReportingIdentifier}";
    }
}