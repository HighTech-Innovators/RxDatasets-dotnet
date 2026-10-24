using System.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProvider.Spanner;

/// <summary>
/// Read/write provider on <see cref="SpannerDatasetStore{T}"/>. Every instance can write: storing a delta succeeds only
/// if it follows the latest version, so an instance that is behind fails its write instead of overwriting, and reloads.
/// The background work (snapshots, cleaning, reporting) runs on one instance only, the holder of a lease row: its
/// readiness notification says whether it leads. Like the read-only provider it polls the versions, every 5 s, so this
/// host's reader also sees the writes of other instances; each poll also renews the lease.
/// </summary>
public class SpannerStorageProvider<T>(
    ILogger<SpannerStorageProvider<T>> logger,
    SpannerDatasetStore<T> store,
    INotificationsHub<T> notificationsHub,
    bool startIfInitFails = true)
    : ReadOnlyObjectStorageProvider<T>(logger, store, notificationsHub, startIfInitFails), IStorageProvider<T>, IHostedService
{
    // ponytail: renewed by the 5 s poll. A leader that can't renew stops its background work 10 s before another host
    // can take the lease. A leader paused longer than that can still overlap with the next one: at-least-once work.
    private static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LeaseMargin = TimeSpan.FromSeconds(10);

    private readonly INotificationsHub<T> _hub = notificationsHub;
    private readonly string _owner = Guid.NewGuid().ToString("N");
    // Orders this host's own writes and the polls in DeltaInfo and Snapshots
    private readonly SemaphoreSlim _state = new(1);
    private DateTimeOffset _leaseValidUntil;
    // The IsLeader of the last Ready sent; null before the first successful initialization, and once stopped
    private bool? _announcedLeader;
    private bool _stopped;

    public new async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        _announcedLeader = null;
        await base.StopAsync(cancellationToken);
        await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
        try
        {
            await store.ReleaseLease(_owner);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Could not release the {Dataset} lease; it expires by itself", typeof(T).Name);
        }
    }

    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data) =>
        StoreDelta(deltaVersion, index, new MemoryStream(data));

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        if (!await store.StoreDelta(deltaVersion, index, data))
        {
            // The writer's index is behind another writer: catch up, then reload it the way a lock handover does
            await Initialize(CancellationToken.None);
            await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
            await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, _announcedLeader ?? false));
            throw new DBConcurrencyException($"Delta {deltaVersion} of {typeof(T).Name} was stored by another writer first");
        }

        using (await _state.LockAsync())
        {
            DeltaInfo.LatestDeltaVersion = Math.Max(DeltaInfo.LatestDeltaVersion, deltaVersion);
            if (DeltaInfo.EarliestDeltaVersion == 0)
            {
                DeltaInfo.EarliestDeltaVersion = deltaVersion;
            }
        }

        await _hub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data) =>
        StoreSnapshot(snapshotVersion, index, new MemoryStream(data));

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        // Losing to another instance is fine here: a snapshot of a version holds the same entities whoever makes it
        await store.StoreSnapshot(snapshotVersion, index, data);
        using (await _state.LockAsync())
        {
            if (Snapshots.All(s => s.Version != snapshotVersion))
            {
                // A new list rather than Add: callers of ListVersions may be enumerating the current one
                Snapshots = [.. Snapshots, new SnapshotType(snapshotVersion, DateTimeOffset.UtcNow)];
            }
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

    /// <summary>Lists the versions, renews the lease, and sends Ready once listed and whenever the leadership changes.</summary>
    protected override async Task Initialize(CancellationToken cancellationToken)
    {
        ulong latestDelta;
        ulong latestSnapshot;
        using (await _state.LockAsync(cancellationToken))
        {
            latestDelta = DeltaInfo.LatestDeltaVersion;
            latestSnapshot = LatestSnapshot();
            // Under the lock: a listing read before one of this host's writes must not set the version back
            await base.Initialize(cancellationToken);
            DeltaInfo.LatestDeltaVersion = Math.Max(DeltaInfo.LatestDeltaVersion, latestDelta);
        }

        // Versions other instances stored; not before Ready, when the subscribers aren't initialized
        if (_announcedLeader is not null && DeltaInfo.LatestDeltaVersion > latestDelta)
        {
            await _hub.Notify(new DatasetUpdatedNotification(DeltaInfo.LatestDeltaVersion));
        }

        if (_announcedLeader is not null && LatestSnapshot() > latestSnapshot)
        {
            await _hub.Notify(new SnapshotCreatedNotification(LatestSnapshot()));
        }

        await RenewLeaseAndAnnounce();
    }

    private async Task RenewLeaseAndAnnounce()
    {
        var requestedAt = DateTimeOffset.UtcNow;
        try
        {
            _leaseValidUntil = await store.TryLease(_owner, LeaseDuration) ? requestedAt + LeaseDuration - LeaseMargin : default;
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Could not renew the {Dataset} lease", typeof(T).Name);
        }

        var leader = DateTimeOffset.UtcNow < _leaseValidUntil;
        if (_stopped || _announcedLeader == leader)
        {
            return;
        }

        _announcedLeader = leader;
        logger.LogInformation("{Dataset} storage is ready; this host {Role} the background work", typeof(T).Name,
            leader ? "runs" : "does not run");
        await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready, leader));
    }

    private ulong LatestSnapshot()
    {
        var snapshots = Snapshots;
        return snapshots.Count > 0 ? snapshots.Max(s => s.Version) : 0;
    }
}
