using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Notifications;

/// <summary>
/// Whether this host runs the dataset's background work (snapshots, cleaning, reporting). Sent by a provider that lets
/// every instance write but elects one for that work; without it, every ready host runs it.
/// </summary>
public readonly record struct LeadershipNotification(bool IsLeader) : INotification;
