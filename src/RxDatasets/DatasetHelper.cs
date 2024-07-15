using System.Collections.Immutable;
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
            var mutableSnapshotIndexBytes = await storageProvider.ReadSnapshotIndex(latestSnapshot);
            var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(mutableSnapshotIndexBytes.Span);

            if (mutableSnapshotIndex.SnapshotVersion != latestSnapshot)
            {
                logger.LogWarning($"Snapshot version {mutableSnapshotIndex.SnapshotVersion} does not match the requested version {latestSnapshot}, overwriting it");
                mutableSnapshotIndex.SnapshotVersion = latestSnapshot;
            }

            snapshotIndex = ImmutableDatasetState.FromProto(mutableSnapshotIndex);
            deltas = await GetChangedDeltas(latestSnapshot, version);
        }
        snapshotIndex = snapshotIndex.ApplyDeltas(deltas);

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

    public static int ComputeGeneralHash(byte[] inputData)
    {
        var hash = new HashCode();
        hash.AddBytes(inputData);
        return hash.ToHashCode();
    }
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
