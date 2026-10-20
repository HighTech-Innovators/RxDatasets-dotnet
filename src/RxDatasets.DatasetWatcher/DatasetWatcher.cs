using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.DatasetWatcher;

public interface IDatasetWatcher<out T>
{
    Task SubscribeAllData(IDatasetObserver<T> datasetObserver, CancellationToken cancellationToken);
}

public class DatasetWatcher<T>(
    ILogger<DatasetWatcher<T>> logger,
    IStorageReader<T> storageReader,
    IOptions<DatasetWatcherConfig> watcherConfig)
    : IDatasetWatcher<T>
{
    private readonly DatasetWatcherConfig _datasetWatcherConfig = watcherConfig.Value;


    private async Task ReportADeltaToTheObserver(IDatasetObserver<T> datasetObserver, ulong nextToReportedVersion)
    {
        await datasetObserver.OnDatasetVersionStart(nextToReportedVersion);
        var deltaIndex = await storageReader.ReadDeltaIndex(nextToReportedVersion);
        var deltaIndexParsed = DeltaIndex.Parser.ParseFrom(deltaIndex.Span);
        deltaIndexParsed.ValidateAgainstVersion(nextToReportedVersion);

        var deltaVersion = deltaIndexParsed.DeltaVersion > 0 ? deltaIndexParsed.DeltaVersion : deltaIndexParsed.DeltaVersionLegacy;
        foreach (var (name, entity) in deltaIndexParsed.EnumerateNewEntities())
        {
            var data = await storageReader.ReadDataFromSources(
                [(ObjectNameTypeEnum.Delta, deltaVersion, entity.Position)], entity.Length);
            await datasetObserver.OnCreate(name, data);
        }

        foreach (var (name, entity) in deltaIndexParsed.EnumerateChangedEntities())
        {
            var data = await storageReader.ReadDataFromSources(
                [(ObjectNameTypeEnum.Delta, deltaVersion, entity.Position)], entity.Length);
            await datasetObserver.OnChange(name, data);
        }

        // The names of the deleted entities, which have the richer info (with previous version)
        var deletedEntityNames = deltaIndexParsed.EnumerateDeletedEntities().Select(e => e.Name).ToList();

        // Only delete via DeletedNames if there's no corresponding DeletedEntity
        // Prefer DeletedEntities as they contain previous version info
        foreach (var entityName in deltaIndexParsed.DeletedNames)
        {
            if (!deletedEntityNames.Contains(entityName))
            {
                await datasetObserver.OnDelete(entityName);
            }
        }

        foreach (var name in deletedEntityNames)
        {
            await datasetObserver.OnDelete(name);
        }

        await datasetObserver.OnDatasetVersionEnd(nextToReportedVersion);

    }

    private static async Task ReportAllDataToTheObserver(IDatasetObserver<T> datasetObserver, SnapshotIndex snapshotIndex,
        ReadOnlyMemory<byte> data)
    {
        var snapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        await datasetObserver.OnDatasetVersionStart(snapshotVersion);

        foreach (var (name, snapshotEntity) in snapshotIndex.EnumerateEntities())
        {
            var entityData = data.Slice((int)snapshotEntity.Position, snapshotEntity.Length);
            var latestChangeType = snapshotEntity.GetLatestChangeType();

            switch (latestChangeType)
            {
                case ChangeType.New:
                    await datasetObserver.OnCreate(name, entityData);
                    break;
                case ChangeType.Changed:
                    await datasetObserver.OnChange(name, entityData);
                    break;
                case ChangeType.Deleted:
                    await datasetObserver.OnDelete(name);
                    break;
                case ChangeType.None:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(latestChangeType), "We are not sure what this Type is");
            }
        }

        await datasetObserver.OnDatasetVersionEnd(snapshotVersion);

    }


    public async Task SubscribeAllData(IDatasetObserver<T> datasetObserver, CancellationToken cancellationToken)
    {
        try
        {
            var versions = await storageReader.ListVersions();
            if (versions.Snapshots.Count == 0)
                return;

            var lastSnapshotVersion = versions.Snapshots.MaxBy(s => s.Version)!.Version;
            var snapshotIndex = await storageReader.ReadSnapshotIndex(lastSnapshotVersion);
            var snapshotIndexParsed = SnapshotIndex.Parser.ParseFrom(snapshotIndex.Span);
            snapshotIndexParsed.ValidateAgainstVersion(lastSnapshotVersion);
            var snapshotData = await storageReader.ReadSnapshotData(lastSnapshotVersion);
            await ReportAllDataToTheObserver(datasetObserver, snapshotIndexParsed, snapshotData);
            var lastReportedVersion = lastSnapshotVersion;

            while (!cancellationToken.IsCancellationRequested)
            {
                lastReportedVersion = await ReportFrom(datasetObserver, lastReportedVersion);
                var checkingInterval = _datasetWatcherConfig.GetCheckingInterval();
                await Task.Delay(checkingInterval, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // intentional
        }
    }

    private async Task<ulong> ReportFrom(IDatasetObserver<T> datasetObserver, ulong version)
    {
        var lastReportedVersion = version;

        try
        {

            var versions = await storageReader.ListVersions();
            if (version < versions.OldestDelta)
            {
                throw new InvalidOperationException(
                    "Requested Version is lower than the oldest available delta version");
            }

            while (true)
            {
                while (version < versions.NewestDelta)
                {
                    version++;
                    await ReportADeltaToTheObserver(datasetObserver, version);
                    lastReportedVersion = version;
                }

                var updatedVersionsMaybe = await storageReader.ListVersions();
                if (updatedVersionsMaybe.NewestDelta == lastReportedVersion)
                {
                    return lastReportedVersion;
                }

                versions = updatedVersionsMaybe;
            }
        }
        catch (Exception e)
        {
            logger.LogWarning(e, e.Message);
            return lastReportedVersion;
        }
    }
}
