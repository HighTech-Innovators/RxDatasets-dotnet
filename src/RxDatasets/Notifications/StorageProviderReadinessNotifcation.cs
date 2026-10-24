using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

/// <param name="IsLeader">
/// Whether this host runs the dataset's background work (snapshots, cleaning, reporting). False on a ready host of a
/// provider that lets every instance write but elects one for that work.
/// </param>
public readonly record struct StorageProviderReadinessNotifcation(ServiceStatus ServiceStatus, bool IsLeader = true)
    : INotification;
