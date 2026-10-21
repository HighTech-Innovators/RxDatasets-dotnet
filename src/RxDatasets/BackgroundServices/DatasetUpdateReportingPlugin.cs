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
                int newCount = deltaIndexParsed.EnumerateNewEntities().Count();
                if (newCount > 0)
                    logger.LogDebug("Processing {NewCount} new entities", newCount);

                int changeCount = deltaIndexParsed.EnumerateChangedEntities().Count();
                if (changeCount > 0)
                    logger.LogDebug("Processing {ChangeCount} changed entities", changeCount);

                int deleteCount = deltaIndexParsed.DeletedNames.Count + deltaIndexParsed.EnumerateDeletedEntities().Count();
                if (deleteCount > 0)
                    logger.LogDebug("Processing {DeleteCount} deleted entities", deleteCount);

                foreach (var (name, newEntity) in deltaIndexParsed.EnumerateNewEntities())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, deltaVersion, newEntity.Position)], newEntity.Length);
                    await _reportingImplementation.OnNewDto(name, data.ToArray(),
                        string.IsNullOrEmpty(newEntity.TrackingId) ? null : newEntity.TrackingId,
                        newEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                    await ReportAliasChanges(name, newEntity.AddedAliases, newEntity.RemovedAliases, newEntity.TrackingId,
                        newEntity.SlaTimestamp, optionalState);
                }

                foreach (var (name, updatedEntity) in deltaIndexParsed.EnumerateChangedEntities())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var data = await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, deltaVersion, updatedEntity.Position)], updatedEntity.Length);

                    await _reportingImplementation.OnChangedDto(name, data.ToArray(),
                        string.IsNullOrEmpty(updatedEntity.TrackingId) ? null : updatedEntity.TrackingId,
                        updatedEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                    await ReportAliasChanges(name, updatedEntity.AddedAliases, updatedEntity.RemovedAliases,
                        updatedEntity.TrackingId, updatedEntity.SlaTimestamp, optionalState);
                }

                foreach (var deletedEntity in deltaIndexParsed.DeletedNames)
                {

                    cancellationToken.ThrowIfCancellationRequested();
                    await _reportingImplementation.OnDeletedDto(deletedEntity,
                        null, null,
                        optionalState);
                }

                foreach (var (name, deletedEntity) in deltaIndexParsed.EnumerateDeletedEntities())
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await _reportingImplementation.OnDeletedDto(name,
                        string.IsNullOrEmpty(deletedEntity.TrackingId) ? null : deletedEntity.TrackingId,
                        deletedEntity.SlaTimestamp?.ToDateTimeOffset(),
                        optionalState);
                    await ReportAliasChanges(name, [], deletedEntity.RemovedAliases, deletedEntity.TrackingId,
                        deletedEntity.SlaTimestamp, optionalState);
                }

                foreach (var change in deltaIndexParsed.AliasChanges)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await ReportAliasChanges(change.Name, change.AddedAliases, change.RemovedAliases, "", null, optionalState);
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

        private async Task ReportAliasChanges(string name, IEnumerable<AliasRef> added, IEnumerable<AliasRef> removed,
            string trackingId, Google.Protobuf.WellKnownTypes.Timestamp? slaTimestamp, object? optionalState)
        {
            var changes = removed.Select(alias => (alias, ChangeType.Deleted)).Concat(added.Select(alias => (alias, ChangeType.New)));
            foreach (var (alias, change) in changes)
            {
                await _reportingImplementation.OnAliasChanged(alias.Name, alias.Value, name, change,
                    string.IsNullOrEmpty(trackingId) ? null : trackingId, slaTimestamp?.ToDateTimeOffset(), optionalState);
            }
        }

        public async Task HandleNewDeltas(CancellationToken stoppingToken)
        {
            await ProcessAllUnreportedDeltas(stoppingToken);
        }

        public string ConsumerIdentifier { get; } = $"DatasetUpdatePlugin-of-{reportingImplementation.ReportingIdentifier}";
    }
}
