using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

public readonly record struct DatasetUpdatedNotification(ulong Version) : INotification;
