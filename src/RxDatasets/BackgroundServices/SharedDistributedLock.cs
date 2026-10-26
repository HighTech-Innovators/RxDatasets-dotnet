using Microsoft.Extensions.DependencyInjection;
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
/// <c>{CustomRootFolder}{lockFolder}/</c> on the storage of the first dataset that joins, so every dataset that joins
/// must use the same storage and root folder. It starts when the first dataset joins and releases the lock when this
/// service stops.
/// </summary>
public sealed class SharedDistributedLock(
    string lockFolder,
    ILoggerFactory loggerFactory,
    IOptions<DistributedLockOptions>? lockOptions = null,
    IClock? clock = null)
    : IDistributedLock, IHostedService, IDisposable
{
    private readonly Lock _sync = new();
    private IDistributedLock? _lock;
    private IHostedService? _lockService;
    private Func<Func<DistributedLockNotification, Task>, IDisposable>? _subscribe;

    public bool HasLock => _lock?.HasLock ?? false;
    public LockServiceState State => _lock?.State ?? LockServiceState.Idle;

    /// <summary>
    /// Joins a dataset: starts the lock if this is the first one, and passes every lock change to
    /// <paramref name="onChange"/>, starting with the current state if there has been a change already.
    /// </summary>
    /// <returns>Disposing it stops passing changes on</returns>
    public IDisposable Join<TEntity>(IDiskStorage<TEntity> storage, StorageConfig storageConfig,
        Func<DistributedLockNotification, Task> onChange)
    {
        lock (_sync)
        {
            if (_lock == null)
            {
                // Its own hub: it replays the last change to a dataset that joins later
                var hub = new NotificationsHub<TEntity>();
                var lockService = new DistributedLockService<TEntity>(storage,
                    Options.Create(new StorageConfig { CustomRootFolder = storageConfig.CustomRootFolder, DtoName = lockFolder }),
                    loggerFactory.CreateLogger<DistributedLockService<TEntity>>(), hub, lockOptions, clock);
                lockService.StartAsync(CancellationToken.None).GetAwaiter().GetResult(); // only starts its loop
                _subscribe = hub.Subscribe;
                _lockService = lockService;
                _lock = lockService;
            }

            return _subscribe!(onChange);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) =>
        _lockService?.StopAsync(cancellationToken) ?? Task.CompletedTask;

    public void Dispose() => (_lockService as IDisposable)?.Dispose();
}

/// <summary>
/// A dataset's view of the instance's <see cref="SharedDistributedLock"/>: it has the lock when the instance has it, and
/// passes the shared lock's changes to the dataset's notifications hub.
/// </summary>
public sealed class SharedDistributedLockMember<TEntity>(
    SharedDistributedLock sharedLock,
    IDiskStorage<TEntity> storage,
    IOptions<StorageConfig> storageConfig,
    INotificationsHub<TEntity> notificationsHub)
    : IDistributedLock<TEntity>, IHostedService, IDisposable
{
    private IDisposable? _subscription;

    public bool HasLock => sharedLock.HasLock;
    public LockServiceState State => sharedLock.State;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _subscription = sharedLock.Join(storage, storageConfig.Value, notificationsHub.Notify);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        // The shared lock flushes only the storage it runs on before it releases the lock
        await storage.FlushAsync();
    }

    public void Dispose() => _subscription?.Dispose();
}

public static class SharedDistributedLockExtensions
{
    /// <summary>
    /// Registers the instance's <see cref="SharedDistributedLock"/>, configured by the <c>DistributedLock</c> section.
    /// Register it before the services that start and stop the datasets, so it stops, and releases the lock, after them.
    /// </summary>
    /// <param name="lockFolder">Folder of the lock files under <c>CustomRootFolder</c>. Instances that share it hand over
    /// to each other, so give every kind of server its own.</param>
    public static IServiceCollection AddSharedDistributedLock(this IServiceCollection services,
        string lockFolder = "SharedLock")
    {
        services.AddValidatedConfigSection<DistributedLockOptions>("DistributedLock");
        services.AddSingleton(sp => new SharedDistributedLock(lockFolder, sp.GetRequiredService<ILoggerFactory>(),
            sp.GetRequiredService<IOptions<DistributedLockOptions>>()));
        services.AddHostedService(sp => sp.GetRequiredService<SharedDistributedLock>());
        return services;
    }
}
