using System;

namespace RxDatasets.Abstractions;

public record HistoryEntry(ulong Version, ChangeType ChangeType, DateTimeOffset Timestamp, ulong PreviousVersion)
{
    public string Id { get; init; } = string.Empty;
}
