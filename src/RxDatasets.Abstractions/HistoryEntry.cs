using System;

namespace RxDatasets.Abstractions;

public record HistoryEntry(ulong Version, ChangeType ChangeType, DateTimeOffset Timestamp, ulong PreviousVersion);
