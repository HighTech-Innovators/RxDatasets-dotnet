using RxDatasets.Abstractions;

namespace RxDatasets.BackgroundServices;

/// <summary>
/// The lock that decides which instance writes a dataset: its own (<see cref="DistributedLockService{TEntity}"/>) or the
/// instance's shared one (<see cref="SharedDistributedLockMember{TEntity}"/>). Changes are announced on the dataset's
/// notifications hub as <see cref="Notifications.DistributedLockNotification"/>.
/// </summary>
public interface IDistributedLock
{
    bool HasLock { get; }
    LockServiceState State { get; }
}

/// <inheritdoc cref="IDistributedLock"/>
// The type parameter keeps the locks of different datasets apart in one service collection
public interface IDistributedLock<TEntity> : IDistributedLock;
