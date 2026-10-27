using System;

namespace RxDatasets.Abstractions;

/// <summary>
/// What the write that made an entity's latest change passed besides the entity: its tracking id (empty when it
/// passed none) and its SLA timestamp.
/// </summary>
public record ChangeOrigin(string TrackingId, DateTimeOffset? SlaTimestamp);
