using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

public readonly record struct StorageProviderReadinessNotifcation(ServiceStatus ServiceStatus) : INotification;
