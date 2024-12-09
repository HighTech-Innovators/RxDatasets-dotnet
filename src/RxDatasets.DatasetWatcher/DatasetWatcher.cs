using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders;

namespace RxDatasets.DatasetWatcher;

public interface IDatasetWatcher<out T>
{
    Task SubscribeAllData(IDatasetObserver<T> datasetObserver, CancellationToken cancellationToken);
}

public class DatasetWatcher<T> : IDatasetWatcher<T>
{
    private readonly ILogger<DatasetWatcher<T>> _logger;
    private readonly IStorageReader<T> _storageReader;
    private readonly DatasetWatcherConfig _datasetWatcherConfig;

    public DatasetWatcher(ILogger<DatasetWatcher<T>> logger, IStorageReader<T> storageReader,
        IOptions<DatasetWatcherConfig> watcherConfig)
    {
        _logger = logger;
        _storageReader = storageReader;
        _datasetWatcherConfig = watcherConfig.Value;
    }


    private async Task ReportADeltaToTheObserver(IDatasetObserver<T> datasetObserver, ulong nextToReportedVersion)
    {
        await datasetObserver.OnDatasetVersionStart(nextToReportedVersion);
        var deltaIndex = await _storageReader.ReadDeltaIndex(nextToReportedVersion);
        var deltaIndexParsed = DeltaIndex.Parser.ParseFrom(deltaIndex.Span);
        foreach (var entity in deltaIndexParsed.NewEntities)
        {
            var data = await _storageReader.ReadDataFromSources(
                [(ObjectNameTypeEnum.DELTA, deltaIndexParsed.DeltaVersion, entity.Position)], entity.Length);
            await datasetObserver.OnCreate(entity.Name, data);
        }

        foreach (var entity in deltaIndexParsed.ChangedEntities)
        {
            var data = await _storageReader.ReadDataFromSources(
                [(ObjectNameTypeEnum.DELTA, deltaIndexParsed.DeltaVersion, entity.Position)], entity.Length);
            await datasetObserver.OnChange(entity.Name, data);
        }

        foreach (var entityName in deltaIndexParsed.DeletedNames)
        {
            await datasetObserver.OnDelete(entityName);
        }

        await datasetObserver.OnDatasetVersionEnd(nextToReportedVersion);

    }

    private async Task ReportAllDataToTheObserver(IDatasetObserver<T> datasetObserver, SnapshotIndex snapshotIndex,
        ReadOnlyMemory<byte> data)
    {

        await datasetObserver.OnDatasetVersionStart(snapshotIndex.SnapshotVersion);

        foreach (var (name, snapshotEntity) in snapshotIndex.Entities)
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
                    throw new ArgumentOutOfRangeException("We are not sure what this Type is");
            }
        }

        await datasetObserver.OnDatasetVersionEnd(snapshotIndex.SnapshotVersion);

    }


    public async Task SubscribeAllData(IDatasetObserver<T> datasetObserver, CancellationToken cancellationToken)
    {
        try
        {
            var versions = await _storageReader.ListVersions();
            if (versions.Snapshots.Count == 0) return;

            var lastSnapshotVersion = versions.Snapshots.MaxBy((s) => s.Version).Version;
            var snapshotIndex = await _storageReader.ReadSnapshotIndex(lastSnapshotVersion);
            var snapshotIndexParsed = SnapshotIndex.Parser.ParseFrom(snapshotIndex.Span);
            var snapshotData = await _storageReader.ReadSnapshotData(lastSnapshotVersion);
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

            var versions = await _storageReader.ListVersions();
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
            
                var updatedVersionsMaybe = await _storageReader.ListVersions();
                if (updatedVersionsMaybe.NewestDelta == lastReportedVersion)
                {
                    return lastReportedVersion;
                }

                versions = updatedVersionsMaybe;
            }
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, e.Message);
            return lastReportedVersion;
        }
    }
}