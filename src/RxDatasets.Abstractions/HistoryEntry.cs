using System;

namespace RxDatasets.Abstractions;

public record HistoryEntry(ulong Version, ChangeType ChangeType, DateTime Timestamp, ulong PreviousVersion);
