using System.Collections.Immutable;
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
        ulong lastSnapshot = availableVersion.snapshots.Count > 0 ? availableVersion.snapshots.Max(x => x.version) : 0;

        return await GetIndexOfVersion(availableVersion.newestDelta, lastSnapshot);
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
    /// <param name="latestSnapshot">The latest snapshot version that is used to speed up the loading</param>
    /// <returns>A snapshot index of the requested version</returns>
    /// <exception cref="InvalidOperationException">In case the requested version is lower than the latestSnapshot</exception>
    public async Task<ImmutableDatasetState> GetIndexOfVersion(ulong version, ulong latestSnapshot)
    {
        ImmutableDatasetState snapshotIndex = new ImmutableDatasetState(new Dictionary<string, ImmutableEntity>().ToImmutableDictionary(), 0, 0);
        List<DeltaIndex> deltas;

        if(latestSnapshot == 0)
        {
            deltas = await GetChangedDeltas(0, version);
        }
        else
        {
            snapshotIndex = await GetLatestSnapshotIndex(latestSnapshot);
            deltas = await GetChangedDeltas(latestSnapshot, version);
        }
        snapshotIndex = snapshotIndex.ApplyDeltas(deltas);

        return snapshotIndex;
    }

    public async Task<ImmutableDatasetState> GetLatestSnapshotIndex(ulong? latestSnapshot = null)
    {
        if(latestSnapshot == null){
            var availableVersion = await storageProvider.ListVersions();
            latestSnapshot = availableVersion.snapshots.Count > 0 ? availableVersion.snapshots.MaxBy((s) => s.version).version : 0ul;
            if (latestSnapshot == 0) throw new FileNotFoundException("There is no latest snapshot.");
        }

        var mutableSnapshotIndexBytes = await storageProvider.ReadSnapshotIndex((ulong)latestSnapshot);
        var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(mutableSnapshotIndexBytes.Span);

        if (mutableSnapshotIndex.SnapshotVersion != latestSnapshot)
        {
            logger.LogWarning($"Snapshot version {mutableSnapshotIndex.SnapshotVersion} does not match the requested version {latestSnapshot}, overwriting it");
            mutableSnapshotIndex.SnapshotVersion = (ulong)latestSnapshot;
        }

        var snapshotIndex = ImmutableDatasetState.FromProto(mutableSnapshotIndex);
        return snapshotIndex;
    }

    public async Task<ImmutableDatasetState> GetSnapshotIndex(ulong deltaVersion, ulong snapshotVersion)
    {
        var availableVersion = await storageProvider.ListVersions();
        ulong lastSnapshot = availableVersion.snapshots.Any((s)=>s.version == snapshotVersion) ? snapshotVersion : 0;
        if (lastSnapshot == 0) throw new InvalidOperationException("Snapshot does not exist");
        return await GetIndexOfVersion(deltaVersion, lastSnapshot);
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
