using System.Collections.Immutable;
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
        return index.Entities
            .Select(x => x.Key)
            .Except(receivedNames);
    }

    public static IEnumerable<string> GetImplicitDeletesForParentNames(ImmutableDatasetState index, HashSet<string> receivedNames,
        HashSet<string> allParents)
    {
        return index.Entities
            .Where(entity => allParents.Contains(FindParentOfEntity(entity.Key) ?? string.Empty))
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
            snapshotIndex = new ImmutableDatasetState(EntityTrie.Empty, 0, version);
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

    /// <summary>
    /// Builds a lightweight position-only lookup for the most recent snapshot,
    /// without constructing the full EntityTrie. Used by readers/writers to find
    /// entity positions in the newest snapshot for data retrieval.
    /// </summary>
    public async Task<SnapshotPositionLookup> GetLatestSnapshotPositionLookup()
    {
        var availableVersions = await storageProvider.ListVersions();

        foreach (var version in availableVersions.Snapshots.Select(s => s.Version).OrderDescending())
        {
            try
            {
                var bytes = await storageProvider.ReadSnapshotIndex(version);
                var proto = SnapshotIndex.Parser.ParseFrom(bytes.Span);
                return SnapshotPositionLookup.FromProto(proto);
            }
            catch (Exception e)
            {
                logger.LogWarning(e,
                    $"Skipping snapshot v{version} position lookup because we got {e}");
            }
        }

        throw new FileNotFoundException("There are no snapshots available to read.");
    }

    private async Task<ImmutableDatasetState> TryGetSnapshot(ulong snapshotVersion)
    {
        var mutableSnapshotIndexBytes = await storageProvider.ReadSnapshotIndex(snapshotVersion);
        var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(mutableSnapshotIndexBytes.Span);
        mutableSnapshotIndex.ValidateAgainstVersion(snapshotVersion);

        // Collect all non-deleted entities from either nested or flat format for validation
        var allEntities = GetAllSnapshotEntities(mutableSnapshotIndex);
        var lastEntityInDataFile = allEntities
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

    /// <summary>
    /// Extracts all SnapshotEntity values from a SnapshotIndex, handling both
    /// nested (trie) and flat (legacy) formats.
    /// </summary>
    internal static IEnumerable<SnapshotEntity> GetAllSnapshotEntities(SnapshotIndex snapshotIndex)
    {
        if (snapshotIndex.Nested != null
            && (snapshotIndex.Nested.Leaves.Count > 0 || snapshotIndex.Nested.Children.Count > 0))
        {
            return CollectNestedSnapshotEntities(snapshotIndex.Nested);
        }

        return snapshotIndex.Entities.Values;
    }

    private static IEnumerable<SnapshotEntity> CollectNestedSnapshotEntities(SnapshotBucket bucket)
    {
        foreach (var entity in bucket.Leaves.Values)
            yield return entity;

        foreach (var child in bucket.Children.Values)
        {
            foreach (var entity in CollectNestedSnapshotEntities(child))
                yield return entity;
        }
    }
    public static string GetObjectNamePrefix<T>(StorageConfig storageConfig) =>
        $"{storageConfig.CustomRootFolder}{(string.IsNullOrEmpty(storageConfig.DtoName) ? typeof(T).Name : storageConfig.DtoName)}/";
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

        // For a single delta, use ReadDeltaIndex to benefit from the per-version cache
        // (e.g. when the reader picks up a delta that was just written through the cache layer).
        // For multiple deltas, use the range method for a single Bigtable scan.
        if (iterations == 1)
        {
            var version = fromVersion + 1;
            var indexBytes = await storageProvider.ReadDeltaIndex(version);
            var singleIndex = NestedIndexHelper.ParseDeltaIndex(indexBytes.Span);
            singleIndex.ValidateAgainstVersion(version);
            return [singleIndex];
        }

        var indexes = new List<DeltaIndex>((int)iterations);
        await foreach (var (version, indexBytes) in storageProvider.ReadDeltaIndexRange(fromVersion, toVersion))
        {
            var index = NestedIndexHelper.ParseDeltaIndex(indexBytes.Span);
            index.ValidateAgainstVersion(version);
            indexes.Add(index);
        }

        return indexes
            .OrderBy(x => x.DeltaVersion).ThenBy(x => x.DeltaVersionLegacy)
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

public class ActionDisposable : IDisposable
{
    private readonly Action _action;

    public ActionDisposable(Action action)
    {
        _action = action ?? throw new ArgumentNullException(nameof(action));
    }

    public void Dispose()
    {
        _action();
    }
}
