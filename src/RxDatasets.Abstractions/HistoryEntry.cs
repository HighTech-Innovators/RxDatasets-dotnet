using System;

namespace RxDatasets.Abstractions;

public record HistoryEntry(ulong Version, ChangeType ChangeType, DateTimeOffset Timestamp, ulong PreviousVersion)
{
    public string Id { get; init; } = string.Empty;

    /// <summary>The tracking id the write that made this change passed; empty when it passed none.</summary>
    public string TrackingId { get; init; } = string.Empty;

    /// <summary>The SLA timestamp the write that made this change passed, if any.</summary>
    public DateTimeOffset? SlaTimestamp { get; init; }
}
