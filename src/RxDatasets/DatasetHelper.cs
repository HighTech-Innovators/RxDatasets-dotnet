using System.Text.RegularExpressions;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;
using RxDatasets.StorageProviders;

namespace RxDatasets;

public class DatasetHelper(ILogger logger, IStorageProviderReader storageProvider)
{
    public async Task<ImmutableDatasetState> GetLatestIndex()
    {
        var availableVersion = await storageProvider.ListVersions();

        return await GetIndexOfVersion(availableVersion.NewestDelta);
    }

    /// <summary>
    /// The parent whose children PutAllChildren replaces, always type/id pairs. Names follow AIP-122: type/id pairs,
    /// optionally ending in a singleton type (AIP-156). The parent drops the last pair, and the singleton type if any:
    /// <c>t/1/s/S/links/A</c> → <c>t/1/s/S</c>, <c>t/1/s/S/esls/B/pages/0/image</c> → <c>t/1/s/S/esls/B</c>, so all
    /// pages of an ESL share one parent. Null when nothing is left.
    /// </summary>
    public static string? FindParentOfEntity(string name)
    {
        int segments = name.AsSpan().Count('/') + 1;
        int drop = segments % 2 == 0 ? 2 : 3;
        if (segments <= drop)
            return null;

        int end = name.Length;
        for (int i = 0; i < drop; i++)
            end = name.LastIndexOf('/', end - 1);
        return name[..end];
    }

    /// <summary>
    /// The parent of an entity PutAllChildren writes. A name of fewer than four segments has no parent, so it can't be
    /// written that way.
    /// </summary>
    /// <exception cref="ArgumentException"><paramref name="name"/> has no parent.</exception>
    public static string ParentForPutAllChildren(string name) =>
        FindParentOfEntity(name) ?? throw new ArgumentException(
            $"PutAllChildren needs a name of at least four segments, so that it has a parent; '{name}' has none",
            nameof(name));

    public static string? FindParentOfEntity(string name, int level)
    {
        if (string.IsNullOrEmpty(name) || level <= 0)
            return null;

        name = name.TrimEnd('/');

        int slashCount = 0;
        for (int i = 0; i < name.Length; i++)
        {
            if (name[i] == '/')
            {
                slashCount++;
                if (slashCount == level)
                    return name.Substring(0, i);
            }
        }

        // If level is greater than the number of segments, return full name
        return slashCount == level - 1 ? name : null;
    }


    public static IEnumerable<string> GetImplicitDeletes(ImmutableDatasetState index, HashSet<string> receivedNames)
    {
        return index.EnumerateAll()
            .Select(x => x.Key)
            .Except(receivedNames);
    }

    public static IEnumerable<string> GetImplicitDeletesForParentNames(ImmutableDatasetState index, HashSet<string> receivedNames,
        HashSet<string> allParents)
    {
        return allParents
            .SelectMany(parent => index.EnumerateAll(parent)
                .Where(entity => FindParentOfEntity(entity.Key) == parent))
            .Select(x => x.Key)
            .Except(receivedNames);
    }

    public static int WriteProtobufLengthPrefix(Stream stream, int length)
    {
        // Google.Protobuf.CodedOutputStream has a method for writing a length prefix as a varint
        using var cos = new CodedOutputStream(stream, true);
        cos.WriteLength(length);
        var lengthOfTheVarPrefix = CodedOutputStream.ComputeLengthSize(length);
        return lengthOfTheVarPrefix;
    }

    /// <summary>
    /// Builds up a SnapshotIndex based on the provided version and the latest snapshot
    /// </summary>
    /// <param name="version">The version of the returned index</param>
    /// <returns>A snapshot index of the requested version</returns>
    /// <exception cref="InvalidOperationException">In case the requested version is lower than the latestSnapshot</exception>
    public async Task<ImmutableDatasetState> GetIndexOfVersion(ulong version)
    {
        List<DeltaIndex> deltas;
        ImmutableDatasetState snapshotIndex;

        try
        {
            snapshotIndex = await GetLatestSnapshotIndex();
        }
        catch (FileNotFoundException)
        {
            // There are no snapshots available
            snapshotIndex = ImmutableDatasetState.Empty(0, version);
        }

        if (snapshotIndex.SnapshotVersion == 0)
        {
            logger.LogInformation($"No Snapshot was seen, Applying deltas from v1 util v{version}");
            deltas = await GetChangedDeltas(0, version);
        }
        else if (version == 0 && snapshotIndex.SnapshotVersion > 0)
        {
            logger.LogInformation($"Snapshot {snapshotIndex.SnapshotVersion} was seen, building index based on it without any extra deltas as we dont have any delta");
            deltas = [];
        }
        else
        {
            logger.LogInformation($"Snapshot {snapshotIndex.SnapshotVersion} was seen, building index based on it to version {version}");
            deltas = await GetChangedDeltas(snapshotIndex.SnapshotVersion, version);
        }

        logger.LogInformation($"Applying {(deltas.Count > 0 ? $"deltas from {deltas.Min(d => d.DeltaVersion)}/(legacy){deltas.Min(d => d.DeltaVersionLegacy)} - {deltas.Max(d => d.DeltaVersion)}/(legacy){deltas.Max(d => d.DeltaVersionLegacy)}" : " no deltas")} to the main index");
        snapshotIndex = snapshotIndex.ApplyDeltas(deltas);

        return snapshotIndex;
    }

    public async Task<ImmutableDatasetState> GetLatestSnapshotIndex(ulong? specificSnapshot = null)
    {
        var availableVersions = await storageProvider.ListVersions();

        if (specificSnapshot != null)
        {
            specificSnapshot = availableVersions.Snapshots.Count > 0
                ? availableVersions.Snapshots.MaxBy((s) => s.Version)!.Version
                : 0ul;
            return await TryGetSnapshot(specificSnapshot.Value);
        }

        foreach (var version in availableVersions.Snapshots.Select(s => s.Version).OrderDescending())
        {
            try
            {
                return await TryGetSnapshot(version);
            }
            catch (Exception e)
            {
                logger.LogWarning(e,
                    $"Skipping snapshot v{version} because we got {e} , we will try the next available snapshot for initialization.");
            }
        }

        throw new FileNotFoundException("There are no snapshots available to read.");
    }

    private async Task<ImmutableDatasetState> TryGetSnapshot(ulong snapshotVersion)
    {
        var mutableSnapshotIndexBytes = await storageProvider.ReadSnapshotIndex(snapshotVersion);
        var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(mutableSnapshotIndexBytes.Span);
        mutableSnapshotIndex.ValidateAgainstVersion(snapshotVersion);
        var lastEntityInDataFile = mutableSnapshotIndex.EnumerateEntities()
            .Select(e => e.Entity)
            .Where(e => e.GetLatestChangeType() != ChangeType.Deleted).MaxBy(e => e.Position);

        if (lastEntityInDataFile is not null)
        {
            var actualSnapshotDataLength = await storageProvider.GetSnapshotDataContentLength(snapshotVersion);
            var expectedLengthFromIndex = lastEntityInDataFile.Position + lastEntityInDataFile.Length;

            if (actualSnapshotDataLength < expectedLengthFromIndex)
            {
                throw new InvalidOperationException(
                    $"Looks like there is a data corruption for snapshot {snapshotVersion}; it's {actualSnapshotDataLength} bytes instead of the expected {expectedLengthFromIndex}");
            }
        }


        var version = mutableSnapshotIndex.SnapshotVersion > 0 ? mutableSnapshotIndex.SnapshotVersion : mutableSnapshotIndex.SnapshotVersionLegacy;
        if (version != snapshotVersion)
        {
            logger.LogWarning(
                $"Snapshot version {version} does not match the requested version {snapshotVersion}, overwriting it");
            mutableSnapshotIndex.SnapshotVersion = snapshotVersion;
        }

        var snapshotIndex = ImmutableDatasetState.FromProto(mutableSnapshotIndex);
        return snapshotIndex;
    }
    public static string GetObjectNamePrefix<T>(StorageConfig storageConfig) =>
        $"{storageConfig.CustomRootFolder}{(string.IsNullOrEmpty(storageConfig.DtoName) ? StorageFolder<T>.Name : storageConfig.DtoName)}/";

    private static readonly Regex NamespaceVersion = new(@"[Vv](\d+)$", RegexOptions.Compiled);

    /// <summary>
    /// The default storage folder of a dataset of <paramref name="entityType"/>: the type name, plus <c>V{n}</c> when the
    /// last namespace segment ends in a version other than 1 (<c>Pricer.Evo.Dtos.LinkV2.Link</c> → <c>LinkV2</c>), so
    /// two versions of a DTO don't share a folder. Version 1 and unversioned types keep the bare type name.
    /// </summary>
    public static string StorageFolderName(Type entityType)
    {
        var match = NamespaceVersion.Match(entityType.Namespace ?? "");
        return match.Success && ulong.Parse(match.Groups[1].Value) != 1
            ? $"{entityType.Name}V{match.Groups[1].Value}"
            : entityType.Name;
    }

    private static class StorageFolder<T>
    {
        public static readonly string Name = StorageFolderName(typeof(T));
    }
    public static string GetObjectName<T>(ulong version, ObjectNameIndexOrDataEnum indexOrData,
        ObjectNameTypeEnum deltaOrSnapshot, StorageConfig storageConfig)
    {
        var extension = indexOrData == ObjectNameIndexOrDataEnum.Index ? "pb" : "pbs";

        var deltaOrSnapshotString = deltaOrSnapshot switch
        {
            ObjectNameTypeEnum.Delta => FolderNames.DeltasFolderName,
            ObjectNameTypeEnum.Snapshot => FolderNames.SnapshotsFolderName,
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameTypeEnum))
        };

        var indexOrDataString = indexOrData switch
        {
            ObjectNameIndexOrDataEnum.Index => "index",
            ObjectNameIndexOrDataEnum.Data => "data",
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameIndexOrDataEnum))
        };

        return
           GetObjectNamePrefix<T>(storageConfig) + $"{deltaOrSnapshotString}/{version}/{indexOrDataString}.{extension}";
    }

    public static string GetObjectFolder<T>(ulong version, ObjectNameTypeEnum deltaOrSnapshot, StorageConfig storageConfig)
    {
        var deltaOrSnapshotString = deltaOrSnapshot switch
        {
            ObjectNameTypeEnum.Delta => FolderNames.DeltasFolderName,
            ObjectNameTypeEnum.Snapshot => FolderNames.SnapshotsFolderName,
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameTypeEnum))
        };

        return GetObjectNamePrefix<T>(storageConfig) + $"{deltaOrSnapshotString}/{version}";
    }
    public async Task<List<DeltaIndex>> GetChangedDeltas(ulong fromVersion, ulong toVersion)
    {
        if (fromVersion > 0 && toVersion == 0)
        {
            return [];
        }

        if (fromVersion > toVersion)
        {
            throw new InvalidOperationException($"fromVersion {fromVersion} is higher than toVersion {toVersion}");
        }

        ulong iterations = (toVersion - fromVersion);

        if (iterations <= 0)
        {
            return [];
        }

        if (iterations > int.MaxValue)
        {
            throw new InvalidOperationException("Too many versions to load");
        }

        List<Task<DeltaIndex>> indexes = Enumerable.Range(1, (int)iterations)
            .Select(async i =>
            {
                var version = fromVersion + (ulong)i;
                var readDeltaIndex = await storageProvider.ReadDeltaIndex(version);
                var index = DeltaIndex.Parser.ParseFrom(readDeltaIndex.Span);
                index.ValidateAgainstVersion(version);
                return index;
            })
            .ToList();

        return (await Task.WhenAll(indexes))
            .OrderBy(x => x.DeltaVersion).ThenBy((x) => x.DeltaVersionLegacy)
            .ToList();
    }

    public static uint ComputeCrc32(ReadOnlySpan<byte> inputData) =>
        inputData.Length == 0 ? 0 : System.IO.Hashing.Crc32.HashToUInt32(inputData);

    public static string GetTempFilePath()
    {
        return Path.Combine(GetTempFileDirectory(), Path.GetRandomFileName());
    }

    public static string GetTempFileDirectory()
    {
        string? tempFolderPath = Environment.GetEnvironmentVariable("TEMP_FILE_DIRECTORY");

        if (string.IsNullOrWhiteSpace(tempFolderPath))
            return Path.GetTempPath();

        Directory.CreateDirectory(tempFolderPath);
        return Path.Combine(tempFolderPath);
    }
}
