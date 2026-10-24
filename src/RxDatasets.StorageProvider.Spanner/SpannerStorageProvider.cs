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
/// The background work (snapshots, cleaning, reporting) runs on one instance only, the holder of a lease row, which it
/// announces with a <see cref="LeadershipNotification"/>. Like the read-only provider it polls the versions, every 5 s, so this
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
    // The last LeadershipNotification sent; null before the first
    private bool? _announcedLeader;
    // Ready sent: after the first successful listing, until stopped
    private bool _ready;
    private bool _stopped;

    public new async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        _ready = false;
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
            await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
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

    /// <summary>
    /// Lists the versions and renews the lease. Sends Ready after the first successful listing, and a
    /// <see cref="LeadershipNotification"/> (before that Ready) whenever the leadership changes.
    /// </summary>
    protected override async Task Initialize(CancellationToken cancellationToken)
    {
        ulong latestDelta;
        ulong latestSnapshot;
        try
        {
            // The whole listing under the lock, so one that read the database before this host stored a version can't
            // set it back. The base class doesn't notify under it: it compares the versions before the asynchronous
            // listing has updated them.
            using (await _state.LockAsync(cancellationToken))
            {
                latestDelta = DeltaInfo.LatestDeltaVersion;
                latestSnapshot = LatestSnapshot();
                await base.Initialize(cancellationToken);
                DeltaInfo.LatestDeltaVersion = Math.Max(DeltaInfo.LatestDeltaVersion, latestDelta);
            }
        }
        catch
        {
            // Also when listing fails: a leader that can't renew must give up the background work in time
            await RenewLease();
            throw;
        }

        await RenewLease();
        if (!_ready)
        {
            if (!_stopped)
            {
                _ready = true;
                await _hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
            }

            return;
        }

        // Versions other instances stored
        if (DeltaInfo.LatestDeltaVersion > latestDelta)
        {
            await _hub.Notify(new DatasetUpdatedNotification(DeltaInfo.LatestDeltaVersion));
        }

        if (LatestSnapshot() > latestSnapshot)
        {
            await _hub.Notify(new SnapshotCreatedNotification(LatestSnapshot()));
        }
    }

    private async Task RenewLease()
    {
        if (_stopped)
        {
            return;
        }

        var requestedAt = DateTimeOffset.UtcNow;
        try
        {
            var leased = await store.TryLease(_owner, LeaseDuration);
            if (_stopped)
            {
                // StopAsync released the lease while this renewal was under way: hand it back again
                if (leased)
                {
                    await store.ReleaseLease(_owner);
                }

                return;
            }

            _leaseValidUntil = leased ? requestedAt + LeaseDuration - LeaseMargin : default;
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Could not renew the {Dataset} lease", typeof(T).Name);
        }

        var leader = DateTimeOffset.UtcNow < _leaseValidUntil;
        if (_announcedLeader == leader)
        {
            return;
        }

        _announcedLeader = leader;
        logger.LogInformation("This host {Role} the {Dataset} background work", leader ? "runs" : "does not run",
            typeof(T).Name);
        await _hub.Notify(new LeadershipNotification(leader));
    }

    private ulong LatestSnapshot()
    {
        var snapshots = Snapshots;
        return snapshots.Count > 0 ? snapshots.Max(s => s.Version) : 0;
    }
}
