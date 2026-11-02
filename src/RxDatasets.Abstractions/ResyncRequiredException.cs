using System;

namespace RxDatasets.Abstractions;

/// <summary>
/// The deltas after <see cref="FromVersion"/> are no longer all kept, so the changes since then, deletes included,
/// can't be told. The caller lists the current state and continues from the version that listing reports, like a
/// Kubernetes watch answered with 410 Gone.
/// </summary>
public class ResyncRequiredException(ulong fromVersion)
    : Exception($"The changes since version {fromVersion} are no longer kept; list the current state and continue from its version")
{
    public ulong FromVersion { get; } = fromVersion;
}
