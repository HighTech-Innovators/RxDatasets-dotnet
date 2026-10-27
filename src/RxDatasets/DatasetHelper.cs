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
    private const string TypeAndId = "[^/]+/[^/]+";

    private static readonly Regex ParentRegex = new(
        $"^({TypeAndId}(/{TypeAndId})*)/[^/]+(/[^/]+)?$",
        RegexOptions.Compiled);

    public async Task<ImmutableDatasetState> GetLatestIndex()
    {
        var availableVersion = await storageProvider.ListVersions();

        return await GetIndexOfVersion(availableVersion.NewestDelta);
    }
    public static string? FindParentOfEntity(string name)
    {
        // Remove any trailing slashes to handle cases like "parent/child/"
        name = name.TrimEnd('/');
        var match = ParentRegex.Match(name);

        return match.Success ? match.Groups[1].Value : null;
    }

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
            .Where(x => x.Value.IsLive)
            .Select(x => x.Key)
            .Except(receivedNames);
    }

    public static IEnumerable<string> GetImplicitDeletesForParentNames(ImmutableDatasetState index, HashSet<string> receivedNames,
        HashSet<string> allParents)
    {
        return allParents
            .SelectMany(parent => index.EnumerateAll(parent)
                .Where(entity => entity.Value.IsLive && FindParentOfEntity(entity.Key) == parent))
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
    /// last namespace segment ends in a version other than 1 (<c>Acme.Dtos.OrderV2.Order</c> → <c>OrderV2</c>), so
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
