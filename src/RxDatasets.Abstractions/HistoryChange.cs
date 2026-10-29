using System;

namespace RxDatasets.Abstractions;

/// <summary>
/// The net change of one name over a range of versions, read from the deltas, with the tracking id (empty when the
/// write passed none) and SLA timestamp of the last change in that range.
/// </summary>
public record HistoryChange(string Id, ChangeType ChangeType, string TrackingId, DateTimeOffset? SlaTimestamp);
