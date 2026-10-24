using System.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProvider.Spanner;

/// <summary>
/// Read/write provider on <see cref="SpannerDatasetStore{T}"/>. It needs no distributed lock: storing a delta inserts
/// its version row, which succeeds only if no other writer stored that version, so each writer instance that is behind
/// fails its write instead of overwriting, and reloads. Like the read-only provider it polls the versions, so this
/// host's reader also sees the writes of other instances.
/// </summary>
public class SpannerStorageProvider<T>(
    ILogger<SpannerStorageProvider<T>> logger,
    SpannerDatasetStore<T> store,
    INotificationsHub<T> notificationsHub,
    bool startIfInitFails = true)
    : ReadOnlyObjectStorageProvider<T>(logger, store, notificationsHub, startIfInitFails), IStorageProvider<T>, IHostedService
{
    private readonly INotificationsHub<T> _hub = notificationsHub;
    private bool _listed;

    public new async Task StartAsync(CancellationToken cancellationToken)
    {
        await base.StartAsync(cancellationToken);
        await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
    }

    public new async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
    }

    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data) => StoreDelta(deltaVersion, index, new MemoryStream(data));

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        if (!await store.StoreDelta(deltaVersion, index, data))
        {
            // The writer's index is behind another writer: catch up, then reload it the way a lock handover does
            await Initialize(CancellationToken.None);
            await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
            await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
            throw new DBConcurrencyException($"Delta {deltaVersion} of {typeof(T).Name} was stored by another writer first");
        }

        DeltaInfo.LatestDeltaVersion = Math.Max(DeltaInfo.LatestDeltaVersion, deltaVersion);
        if (DeltaInfo.EarliestDeltaVersion == 0)
        {
            DeltaInfo.EarliestDeltaVersion = deltaVersion;
        }

        await _hub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data) =>
        StoreSnapshot(snapshotVersion, index, new MemoryStream(data));

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        // Losing to another instance is fine here: a snapshot of a version holds the same entities whoever makes it
        await store.StoreSnapshot(snapshotVersion, index, data);
        if (Snapshots.All(s => s.Version != snapshotVersion))
        {
            Snapshots.Add(new SnapshotType(snapshotVersion, DateTimeOffset.UtcNow));
        }

        await _hub.Notify(new SnapshotCreatedNotification(LatestSnapshot()));
    }

    public async Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas)
    {
        if (Snapshots.All(s => s.Version != snapshotVersion))
        {
            throw new InvalidOperationException($"Snapshot version {snapshotVersion} does not exist");
        }

        logger.LogInformation("Storage cleaning: deleting everything older than version {Version}", snapshotVersion);
        await store.DeleteOlderThan(snapshotVersion, keepAllDeltas);
        await Initialize(CancellationToken.None);
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        await store.StoreProcessedVersion(who, version);
        await _hub.Notify(new VersionProcessedNotification(who, version));
    }

    public Task<ulong> GetProcessedVersion(string who) => store.GetProcessedVersion(who);

    public Task<ulong> GetLowestProcessedVersion() => store.GetLowestProcessedVersion();

    protected override async Task Initialize(CancellationToken cancellationToken)
    {
        var latestDelta = DeltaInfo.LatestDeltaVersion;
        var latestSnapshot = LatestSnapshot();
        await base.Initialize(cancellationToken);

        // Versions another instance stored; the base class misses them when the listing is asynchronous
        if (_listed && DeltaInfo.LatestDeltaVersion > latestDelta)
        {
            await _hub.Notify(new DatasetUpdatedNotification(DeltaInfo.LatestDeltaVersion));
        }

        if (_listed && LatestSnapshot() > latestSnapshot)
        {
            await _hub.Notify(new SnapshotCreatedNotification(LatestSnapshot()));
        }

        _listed = true;
    }

    private ulong LatestSnapshot()
    {
        var snapshots = Snapshots;
        return snapshots.Count > 0 ? snapshots.Max(s => s.Version) : 0;
    }
}
