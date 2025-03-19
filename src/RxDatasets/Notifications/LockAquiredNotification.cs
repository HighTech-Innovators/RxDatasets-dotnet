using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

public readonly record struct DistributedLockNotification(DistirbutedLockStatus LockStatus) : INotification;
