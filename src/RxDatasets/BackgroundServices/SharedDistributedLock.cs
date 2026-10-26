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
/// starts when the first dataset joins and releases the lock when this service stops.
/// </summary>
public sealed class SharedDistributedLock : IDistributedLock, IHostedService, IDisposable
{
    private readonly Lock _sync = new();
    private readonly DistributedLockService<SharedDistributedLock> _lock;
    private readonly IDisposable _subscription;
    private readonly List<Func<DistributedLockNotification, Task>> _members = [];
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
    /// service has stopped, a dataset that joins gets no changes.
    /// </summary>
    /// <returns>Disposing it stops passing changes on</returns>
    public IDisposable Join(Func<DistributedLockNotification, Task> onChange)
    {
        lock (_sync)
        {
            if (!_started && !_stopped)
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
                _members.Remove(onChange);
        });
    }

    private Task Forward(DistributedLockNotification change)
    {
        Task[] sent;
        lock (_sync)
        {
            if (_stopped)
                return Task.CompletedTask;
            _last = change;
            sent = _members.Select(onChange => onChange(change)).ToArray();
        }

        return Task.WhenAll(sent);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            _stopped = true;
            _last = null; // it releases the lock below, so a dataset that joins later must not be told Acquried
            if (!_started)
                return Task.CompletedTask;
        }

        return _lock.StopAsync(cancellationToken);
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
    INotificationsHub<TEntity> notificationsHub)
    : IDistributedLock<TEntity>, IHostedService, IDisposable
{
    private IDisposable? _subscription;

    public bool HasLock => sharedLock.HasLock;
    public LockServiceState State => sharedLock.State;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _subscription = sharedLock.Join(notificationsHub.Notify);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        // The shared lock flushes only its own storage before it releases the lock
        await storage.FlushAsync();
    }

    public void Dispose() => _subscription?.Dispose();
}

public static class SharedDistributedLockExtensions
{
    /// <summary>
    /// Registers the instance's <see cref="SharedDistributedLock"/> on a <typeparamref name="TStorage"/> of its own, with
    /// the root folder of the <c>Storage</c> section and the options of the <c>DistributedLock</c> section. The storage's
    /// own section (e.g. <c>GoogleCloud</c>) must be registered too, as a dataset's <c>With…Storage()</c> does. Register
    /// it before the services that start and stop the datasets: a host that stops its services one after the other (not
    /// <c>HostOptions.ServicesStopConcurrently</c>) then stops it, and releases the lock, after them.
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
        services.AddSingleton(sp => ActivatorUtilities.CreateInstance<SharedDistributedLock>(sp, lockFolder));
        services.AddHostedService(sp => sp.GetRequiredService<SharedDistributedLock>());
        return services;
    }
}
