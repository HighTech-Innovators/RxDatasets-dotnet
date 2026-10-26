using System.Reactive.Disposables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.BackgroundServices;

/// <summary>
/// One distributed lock for every dataset of this instance that joins it
/// (<see cref="DtoServerBuilder{TEntity}.WithSharedDistributedLock"/>): the instance that holds it writes all of them, and
/// a handover passes all of them at once. The lock is a <see cref="DistributedLockService{TEntity}"/> with its files in
/// <c>{CustomRootFolder}{lockFolder}/</c> on a storage of its own, which must reach the store the datasets use. That
/// storage is conditional (compare-and-swap) by type, and belongs to whoever registered it, not to a dataset. The lock
/// starts when the first dataset joins, and releases the lock when this service has stopped and every dataset has left.
/// </summary>
public sealed class SharedDistributedLock : IDistributedLock, IHostedService, IDisposable
{
    private readonly Lock _sync = new();
    private readonly DistributedLockService<SharedDistributedLock> _lock;
    private readonly IDisposable _subscription;
    private readonly List<Func<DistributedLockNotification, Task>> _members = [];
    private readonly TaskCompletionSource _allLeft = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ILogger _logger;
    private DistributedLockNotification? _last;
    private bool _started;
    private bool _stopped;

    public SharedDistributedLock(
        IConditionalDiskStorage<SharedDistributedLock> storage,
        IOptions<StorageConfig> storageConfig,
        string lockFolder,
        ILoggerFactory loggerFactory,
        IOptions<DistributedLockOptions>? lockOptions = null,
        IClock? clock = null)
    {
        _logger = loggerFactory.CreateLogger<SharedDistributedLock>();
        // Its only subscriber, there before any change: the lock's changes reach Forward once and in order
        var hub = new NotificationsHub<SharedDistributedLock>();
        _subscription = hub.Subscribe<DistributedLockNotification>(Forward);
        _lock = new DistributedLockService<SharedDistributedLock>(storage,
            Options.Create(new StorageConfig { CustomRootFolder = storageConfig.Value.CustomRootFolder, DtoName = lockFolder }),
            loggerFactory.CreateLogger<DistributedLockService<SharedDistributedLock>>(), hub, lockOptions, clock);
    }

    public bool HasLock => _lock.HasLock;
    public LockServiceState State => _lock.State;

    /// <summary>
    /// Joins a dataset: starts the lock if this is the first one, and passes every lock change to
    /// <paramref name="onChange"/>, starting with the current state if there has been a change already. Once this
    /// service is stopping, a dataset that joins gets no changes. <paramref name="onChange"/> runs under this lock's own
    /// lock to keep the changes in order, so it must only queue the change and not block, as
    /// <see cref="NotificationsHub{T}.Notify{TNotification}"/> does.
    /// </summary>
    /// <returns>Disposing it stops passing changes on</returns>
    public IDisposable Join(Func<DistributedLockNotification, Task> onChange)
    {
        lock (_sync)
        {
            if (_stopped)
                return Disposable.Empty;
            if (!_started)
            {
                _started = true;
                _lock.StartAsync(CancellationToken.None).GetAwaiter().GetResult(); // only starts its loop
            }

            _members.Add(onChange);
            // Under _sync, as in Forward: a dataset's hub queues the change before Notify returns, so the current
            // state can't arrive after a newer change
            if (_last is { } last)
                _ = onChange(last);
        }

        return Disposable.Create(() =>
        {
            lock (_sync)
            {
                _members.Remove(onChange);
                if (_stopped && _members.Count == 0)
                    _allLeft.TrySetResult();
            }
        });
    }

    private Task Forward(DistributedLockNotification change)
    {
        Task[] sent;
        lock (_sync)
        {
            // While it stops, the datasets that haven't left yet still write, so they still get a Lost
            if (!_stopped)
                _last = change;
            sent = _members.Select(onChange => onChange(change)).ToArray();
        }

        return Task.WhenAll(sent);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Waits until every dataset has left (stopped and flushed), then releases the lock. When
    /// <paramref name="cancellationToken"/> fires first, it releases the lock anyway. A host that stops its services
    /// concurrently may stop this one before or with the datasets; a host that stops them one after the other must stop
    /// it after them (register it before them), or this waits for datasets that it doesn't stop until this returns.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            _stopped = true;
            _last = null; // it releases the lock below, so a dataset that joins later must not be told Acquried
            if (!_started)
                return;
            if (_members.Count == 0)
                _allLeft.TrySetResult();
        }

        try
        {
            await _allLeft.Task.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Releasing the shared lock while datasets still hold it: the stop timed out");
        }

        await _lock.StopAsync(cancellationToken);
    }

    public void Dispose()
    {
        _subscription.Dispose();
        _lock.Dispose();
    }
}

/// <summary>
/// A dataset's view of the instance's <see cref="SharedDistributedLock"/>: it has the lock when the instance has it, and
/// passes the shared lock's changes to the dataset's notifications hub.
/// </summary>
public sealed class SharedDistributedLockMember<TEntity>(
    SharedDistributedLock sharedLock,
    IDiskStorage<TEntity> storage,
    INotificationsHub<TEntity> notificationsHub,
    ILogger<SharedDistributedLockMember<TEntity>> logger)
    : IDistributedLock<TEntity>, IHostedService, IDisposable
{
    private IDisposable? _subscription;
    private volatile bool _left;

    // Once it stops it no longer has the lock, though the instance may still hold it for other datasets: a write that
    // comes after its flush would not be flushed before the lock is released
    public bool HasLock => !_left && sharedLock.HasLock;
    public LockServiceState State => _left ? LockServiceState.Idle : sharedLock.State;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _subscription = sharedLock.Join(notificationsHub.Notify);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _left = true;
        // The shared lock flushes only its own storage, and releases the lock once every dataset has left
        try
        {
            await storage.FlushAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error flushing storage write queue before leaving the shared lock.");
        }

        Dispose();
    }

    public void Dispose()
    {
        _left = true;
        _subscription?.Dispose();
    }
}

public static class SharedDistributedLockExtensions
{
    /// <summary>
    /// Registers the instance's <see cref="SharedDistributedLock"/> on a <typeparamref name="TStorage"/> of its own, with
    /// the root folder of the <c>Storage</c> section and the options of the <c>DistributedLock</c> section. The storage's
    /// own section (e.g. <c>GoogleCloud</c>) must be registered too, as a dataset's <c>With…Storage()</c> does. It
    /// releases the lock only once every dataset has stopped, so register it before the services that start and stop the
    /// datasets: a host that stops its services one after the other then stops it after them. Only with
    /// <c>HostOptions.ServicesStopConcurrently</c> does the order not matter. The storage is also
    /// registered as <c>IDiskStorage&lt;SharedDistributedLock&gt;</c>, so the provider's health check can watch it
    /// (e.g. <c>AddCheck&lt;GoogleCloudHealthCheck&lt;SharedDistributedLock&gt;&gt;</c>); nothing else does.
    /// </summary>
    /// <typeparam name="TStorage">The datasets' kind of storage, e.g. <c>GoogleCloudStorage&lt;SharedDistributedLock&gt;</c>;
    /// only a conditional one will do.</typeparam>
    /// <param name="lockFolder">Folder of the lock files under <c>CustomRootFolder</c>. Instances that share it hand over
    /// to each other, so give every kind of server its own.</param>
    public static IServiceCollection AddSharedDistributedLock<TStorage>(this IServiceCollection services,
        string lockFolder = "SharedLock")
        where TStorage : class, IConditionalDiskStorage<SharedDistributedLock>
    {
        services.AddValidatedConfigSection<StorageConfig>();
        services.AddValidatedConfigSection<DistributedLockOptions>("DistributedLock");
        services.TryAddSingleton<IConditionalDiskStorage<SharedDistributedLock>, TStorage>();
        services.TryAddSingleton<IDiskStorage<SharedDistributedLock>>(sp =>
            sp.GetRequiredService<IConditionalDiskStorage<SharedDistributedLock>>());
        services.AddSingleton(sp => ActivatorUtilities.CreateInstance<SharedDistributedLock>(sp, lockFolder));
        services.AddHostedService(sp => sp.GetRequiredService<SharedDistributedLock>());
        return services;
    }
}
