using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;
public readonly record struct VersionProcessedNotification(string Who, ulong NewVersion) : INotification;
