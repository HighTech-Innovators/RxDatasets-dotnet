
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageProviders;

public class ObjectStorageReader<T>(
    ILogger<ObjectStorageReader<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IDiskStorage<T> diskStorage
    )
    : IStorageReader<T>
{
    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        throw new NotImplementedException();
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var objectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return diskStorage.DownloadBytes(objectName);
    }
    
    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        var objectName = DatasetHelper.GetObjectName<T>(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return diskStorage.DownloadWithCallBack(objectName, callback);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var objectName = DatasetHelper.GetObjectName<T>(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var bytes = await diskStorage.DownloadBytes(objectName);

        return bytes;
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var objectName = DatasetHelper.GetObjectName<T>(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        var bytes = await diskStorage.DownloadBytes(objectName);
        return bytes;
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        var (deltaInfo, snapshotInfo) = await ReadVersionInfo();
        var snapshots = snapshotInfo.SnapshotVersions
            .Select(v => new SnapshotType(v.Version, v.Timestamp.ToDateTimeOffset()))
            .ToList();
        return (deltaInfo.EarliestDeltaVersion,
            deltaInfo.LatestDeltaVersion, snapshots);
    }
    

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
            throw new ArgumentException("at least one source must be filled");

        foreach (var (type, version, position) in sources)
        {
            var objectName = DatasetHelper.GetObjectName<T>(version, ObjectNameIndexOrDataEnum.DATA, type, storageConfig.Value);
            return await diskStorage.DownloadBytes(objectName, position, length);
        }

        throw new ArgumentException("None of the sources is available");
    }
    
  
    private string GetDeltaInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + "DeltaInfo.pb";
    }

    private string GetSnapshotInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + "SnapshotInfo.pb";
    }

    private async Task<(DeltaInfo deltaInfo, SnapshotInfo snapshotInfo)> ReadVersionInfo()
    {
        try
        {
            logger.LogDebug($"Attempting to retrieve latest version from Delta and Snapshot Info files. for {typeof(T).Name}");

            var deltaTask = DownloadAndParseProtobufFileAsync<DeltaInfo>(GetDeltaInfoFilename(), DeltaInfo.Parser);
            var snapshotTask = DownloadAndParseProtobufFileAsync<SnapshotInfo>(GetSnapshotInfoFilename(), SnapshotInfo.Parser);

            // Use Task.WhenAll to perform file reads concurrently
            //  This ensures that we don't pass when only one of them succeeds
            await Task.WhenAll(deltaTask, snapshotTask);

            var deltaInfo = await deltaTask;
            var snapshotInfo = await snapshotTask;

            if (VersionsAreCorrupted(deltaInfo, snapshotInfo)) 
                (deltaInfo, snapshotInfo) = await CreateLatestVersionFromObjectLists();
            
                
            logger.LogDebug($"loaded DeltaInfo " +
                            $"(oldest v = {deltaInfo.EarliestDeltaVersion}, latest v = {deltaInfo.LatestDeltaVersion} )" +
                            $" and SnapshotInfo [{string.Join(",", snapshotInfo.SnapshotVersions.Select(s=>s.Version))}] " +
                            $"files for {typeof(T).Name}.");
            
            return (deltaInfo, snapshotInfo);
        }
        catch (FileNotFoundException ex)
        {
            logger.LogDebug($"FileNotFoundException: {ex.FileName}. Delta or Snapshot file not found.");
            return await TryGetVersionFromFallbacks();
        }
    }

    private static bool VersionsAreCorrupted(DeltaInfo deltaInfo, SnapshotInfo snapshotInfo)
    {
        return (deltaInfo.EarliestDeltaVersion == 0 || (snapshotInfo.SnapshotVersions.Count > 0 &&
                                                        deltaInfo.EarliestDeltaVersion >
                                                        snapshotInfo.SnapshotVersions.Max(s => s.Version)+1));

    }

    private async Task<(DeltaInfo deltaInfo, SnapshotInfo snapshotInfo)> TryGetVersionFromFallbacks()
    {
        logger.LogInformation("Attempting to retrieve version from fallback files.");

        var versionV2 = await TryParseVersionFileAsync<ObsoleteV2VersionInfo>("VersionIndex.pb", ObsoleteV2VersionInfo.Parser);
        if (versionV2 != null)
        {
            logger.LogInformation($"Successfully retrieved version from VersionIndexV2 for {typeof(T).Name}.");
            return ReadVersionFromV2Data(versionV2);
            
        }

        logger.LogInformation("VersionIndexV2 not found, attempting to retrieve version from VersionIndexV1.");
        var versionV1 = await TryParseVersionFileAsync<ObsoleteV1LatestVersion>("latestVersion.pb", ObsoleteV1LatestVersion.Parser);
        if (versionV1 != null)
        {
            logger.LogInformation($"Successfully retrieved version from VersionIndexV1 for {typeof(T).Name}.");
            return CreateVersionFromV1Data(versionV1);
        }

        logger.LogInformation($"No valid fallback versions found. Creating new latest version for {typeof(T).Name}.");
        return await CreateLatestVersionFromObjectLists();
    }

    private async Task<TMessage?> TryParseVersionFileAsync<TMessage>(string filenameSuffix,
        MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        var filename = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + filenameSuffix;
        try
        {
            logger.LogInformation($"Attempting to parse file: {filename}");
            return await DownloadAndParseProtobufFileAsync(filename, parser);
        }
        catch (FileNotFoundException ex)
        {
            logger.LogInformation($"FileNotFoundException: {ex.Message}. File {filename} not found.");
            return default;
        }
    }

    private (DeltaInfo deltaInfo, SnapshotInfo snapshotInfo) ReadVersionFromV2Data(ObsoleteV2VersionInfo versionIndexDataV2)
    {
        logger.LogInformation("Creating version from VersionIndexV2 data.");
        

        var deltaInfo = new DeltaInfo
        {
            LatestDeltaVersion = versionIndexDataV2.LatestDeltaVersion,
            EarliestDeltaVersion = versionIndexDataV2.EarliestDeltaVersion
        };

        var snapshotInfo = new SnapshotInfo
            {
                SnapshotVersions =
                {
                    versionIndexDataV2.SnapshotVersions.Select(s => new SnapshotInfo.Types.snapshotVersion
                        { Timestamp = s.Timestamp, Version = s.Version })
                }
            };
        
        logger.LogInformation("Version created successfully from VersionIndexV2.");
        return (deltaInfo, snapshotInfo);
    }

    private (DeltaInfo deltaInfo, SnapshotInfo snapshotInfo) CreateVersionFromV1Data(ObsoleteV1LatestVersion versionIndexDataV1)
    {
        logger.LogInformation("Creating version from VersionIndexV1 data.");

        var deltaInfo =
            new DeltaInfo
            {
                LatestDeltaVersion = versionIndexDataV1.LatestDeltaVersion
            };
        var snapshotInfo = new SnapshotInfo
        {
            SnapshotVersions =
            {
                new SnapshotInfo.Types.snapshotVersion
                {
                    Timestamp = DateTimeOffset.UtcNow.ToTimestamp(),
                    Version = versionIndexDataV1.LatestSnapshotVersion
                }
            }
        };
        
        logger.LogInformation("Version created successfully from VersionIndexV1.");
        return (deltaInfo, snapshotInfo);   
    }

    private async Task<TMessage> DownloadAndParseProtobufFileAsync<TMessage>(string filename, MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        var fileBytes = await diskStorage.DownloadBytes(filename);
        return parser.ParseFrom(fileBytes.Span);
    }

    private async Task<(DeltaInfo deltaInfo, SnapshotInfo snapshotInfo)> CreateLatestVersionFromObjectLists()
    {
        var objectNamePrefix = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + FolderNames.DeltasFolderNameWithSlash;
        var listObjects = await diskStorage.ListObjects(objectNamePrefix);
        var deltas = listObjects.Select(d => SharedMethods.ExtractNumber(d.ObjectKey))
            .Order()
            .GroupBy(v=>v)
            .Select(g => g.First())
            .ToList();

        var listOfSnapshots = await diskStorage.ListObjects(DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + FolderNames.SnapshotsFolderNameWithSlash);
        var snapshots = listOfSnapshots.Select(s=> (Version: (ulong)SharedMethods.ExtractNumber(s.ObjectKey), s.DateTime))
            .OrderBy(d => d.Version)
            .GroupBy(g => g.Version)
            .Select(g => g.First())
            .ToList();

        var deltaInfo = new DeltaInfo();
        var snapshotInfo = new SnapshotInfo();

        deltaInfo.EarliestDeltaVersion = deltas.Count > 0 ? (ulong)deltas.First(): 0ul;
        deltaInfo.LatestDeltaVersion = deltas.Count > 0 ? (ulong)deltas.Last(): 0ul;

        snapshotInfo.SnapshotVersions.AddRange(snapshots.Select(s =>
           new SnapshotInfo.Types.snapshotVersion
           {
               Timestamp = Timestamp.FromDateTime(s.DateTime.UtcDateTime),
               Version = s.Version
           }));

        logger.LogInformation($"We scanned up {snapshots.Count} snapshots [{string.Join(",", snapshots.Select(s => s.Version))}]" +
                              $" and {deltas.Count} deltas, v({deltaInfo.EarliestDeltaVersion}, {deltaInfo.LatestDeltaVersion}))");
        return (deltaInfo, snapshotInfo);
    }
    
    
    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var objectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return diskStorage.GetObjectContentLength(objectName);
    }
}