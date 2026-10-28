namespace RxDatasets.Abstractions;

/// <summary>
/// A reporter that takes several waiting deltas in one transaction. The reporting plugin puts up to
/// <see cref="MaxDeltasPerTransaction"/> consecutive deltas between one <c>OpenDeltaTransaction</c> and one
/// <c>CommitDeltaTransaction</c>, and writes the processed version once for all of them. A reporter that does not
/// implement this interface gets one delta per transaction.
/// </summary>
public interface IBatchedDatasetUpdateReporting<T> : IDatasetUpdateReporting<T>
{
    /// <summary>The most deltas the plugin puts into one transaction. At least 1.</summary>
    int MaxDeltasPerTransaction { get; }
}
