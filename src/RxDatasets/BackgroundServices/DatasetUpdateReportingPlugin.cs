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
        private readonly TReporter _reportingImplementation = reportingImplementation;
        private string ReportingIdentifier => _reportingImplementation.ReportingIdentifier;

        private async Task ProcessAllUnreportedDeltas(CancellationToken stoppingToken)
        {
            var newestDelta = (await storageProvider.ListVersions()).NewestDelta;
            var lastProcessedDelta = await storageProvider.GetProcessedVersion(ReportingIdentifier);

            while (!stoppingToken.IsCancellationRequested && lastProcessedDelta < newestDelta)
            {
                var nextVersionToProcess = lastProcessedDelta + 1;
                try
                {
                    await ProcessDelta(nextVersionToProcess, stoppingToken);
                    lastProcessedDelta++;
                    await storageProvider.StoreProcessedVersion(ReportingIdentifier, lastProcessedDelta);
                    logger.LogInformation("Processed delta {Version}", nextVersionToProcess);
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    logger.LogWarning(e, "Something went wrong while Processing delta {Version}; retrying", nextVersionToProcess);
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
                deltaIndexParsed.ValidateAgainstVersion(deltaVersion);

            }
            catch (FileNotFoundException)
            {
                logger.LogWarning(
                    "Index file for delta {Version} doesn't exist. Skipping.", deltaVersion);
                return;
            }

            logger.LogDebug("Processing delta {Version}", deltaVersion);
            var optionalState = await _reportingImplementation.OpenDeltaTransaction(deltaVersion);
            try
            {
                if (deltaIndexParsed.NewEntities.Count > 0)
                    logger.LogDebug("Processing {NewCount} new entities", deltaIndexParsed.NewEntities.Count);

                if (deltaIndexParsed.ChangedEntities.Count > 0)
                    logger.LogDebug("Processing {ChangeCount} changed entities", deltaIndexParsed.ChangedEntities.Count);

                if (deltaIndexParsed.DeletedNames.Count > 0 || deltaIndexParsed.DeletedEntities.Count > 0)
                    logger.LogDebug("Processing {DeleteCount} deleted entities", deltaIndexParsed.DeletedNames.Count + deltaIndexParsed.DeletedEntities.Count);

                foreach (var newEntity in deltaIndexParsed.NewEntities)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, deltaVersion, newEntity.Position)], newEntity.Length);
                    await _reportingImplementation.OnNewDto(newEntity.Name, data.ToArray(),
                        string.IsNullOrEmpty(newEntity.TrackingId) ? null : newEntity.TrackingId,
                        newEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                }

                foreach (var updatedEntity in deltaIndexParsed.ChangedEntities)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, deltaVersion, updatedEntity.Position)], updatedEntity.Length);

                    await _reportingImplementation.OnChangedDto(updatedEntity.Name, data.ToArray(),
                        string.IsNullOrEmpty(updatedEntity.TrackingId) ? null : updatedEntity.TrackingId,
                        updatedEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                }

                foreach (var deletedEntity in deltaIndexParsed.DeletedNames)
                {

                    cancellationToken.ThrowIfCancellationRequested();
                    await _reportingImplementation.OnDeletedDto(deletedEntity,
                        null, null,
                        optionalState);
                }

                foreach (var deletedEntity in deltaIndexParsed.DeletedEntities)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _reportingImplementation.OnDeletedDto(deletedEntity.Name,
                        string.IsNullOrEmpty(deletedEntity.TrackingId) ? null : deletedEntity.TrackingId,
                        deletedEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                }

                if (optionalState != null)
                {
                    logger.LogDebug("Committing my delta {Version} transaction", deltaVersion);
                    await _reportingImplementation.CommitDeltaTransaction(optionalState);
                    optionalState = null; // Prevent rollback in finally
                }
            }
            catch (FileNotFoundException)
            {
                logger.LogWarning(
                    "Data files for delta {Version} don't exist. Either data is corrupted or is deleted. Skipping.", deltaVersion);
            }
            finally
            {
                if (optionalState != null)
                {
                    logger.LogInformation("Rolling back my delta {Version} transaction", deltaVersion);
                    await _reportingImplementation.RollBackDeltaTransaction(optionalState);
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
