using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets;

public partial class DatasetHelper(ILogger logger, IStorageProvider storageProvider)
{
    public async Task<ImmutableDatasetState> GetLatestIndex()
    {
        var availableVersion = await storageProvider.ListVersions();

        return await GetIndexOfVersion(availableVersion.newestDelta);
    }


    public static int WriteLengthPrefix(Stream stream, int length)
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
        catch (FileNotFoundException e)
        {
            // There are no snapshots available
            snapshotIndex = new ImmutableDatasetState(new Dictionary<string, ImmutableEntity>().ToImmutableDictionary(), 0, version);
        }

        if (snapshotIndex.SnapshotVersion == 0)
        {
            deltas = await GetChangedDeltas(0, version);
        }
        else
        {
            deltas = await GetChangedDeltas(snapshotIndex.SnapshotVersion, version);
        }

        snapshotIndex = snapshotIndex.ApplyDeltas(deltas);

        return snapshotIndex;
    }

    public async Task<ImmutableDatasetState> GetLatestSnapshotIndex(ulong? specificSnapshot = null)
    {
        var availableVersions = await storageProvider.ListVersions();

        if (specificSnapshot != null){
            specificSnapshot = availableVersions.snapshots.Count > 0 ? availableVersions.snapshots.MaxBy((s) => s.version).version : 0ul;
            return await TryGetSnapshot(specificSnapshot.Value);
        }
        
        foreach (var version in availableVersions.snapshots.Select(s => s.version).OrderDescending())
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

        if (mutableSnapshotIndex.SnapshotVersion != snapshotVersion)
        {
            logger.LogWarning($"Snapshot version {mutableSnapshotIndex.SnapshotVersion} does not match the requested version {snapshotVersion}, overwriting it");
            mutableSnapshotIndex.SnapshotVersion = snapshotVersion;
        }

        var snapshotIndex = ImmutableDatasetState.FromProto(mutableSnapshotIndex);
        return snapshotIndex;
    }

    public async Task<List<DeltaIndex>> GetChangedDeltas(ulong fromVersion, ulong toVersion)
    {
        if(fromVersion > toVersion)
        {
            throw new InvalidOperationException("fromVersion is higher than toVersion");
        }

        ulong itterations = (toVersion - fromVersion);

        if(itterations <= 0)
        {
            return new List<DeltaIndex>();
        }

        if(itterations > int.MaxValue)
        {
            throw new InvalidOperationException("Too many versions to load");
        }

        List<Task<DeltaIndex>> indexes = Enumerable.Range(1, (int)itterations)
            .Select(async i =>
            {
                var index = DeltaIndex.Parser.ParseFrom((await storageProvider.ReadDeltaIndex(fromVersion + (ulong)i)).Span);
                return index;
            })
            .ToList();

        return (await Task.WhenAll(indexes))
            .OrderBy(x => x.DeltaVersion)
            .ToList();
    }

    public static uint ComputeCrc32(ReadOnlySpan<byte> inputData) => inputData.Length == 0 ? 0 : System.IO.Hashing.Crc32.HashToUInt32(inputData);
}


internal class ActionDisposable : IDisposable
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
