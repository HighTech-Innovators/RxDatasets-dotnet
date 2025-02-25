using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

public readonly record struct SnapshotCreatedNotification(ulong Version) : INotification;
