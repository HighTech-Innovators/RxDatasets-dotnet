using RxDatasets.Abstractions;

namespace RxDatasets.Reporting.Cqs;

/// <summary>
/// A reporter that wants several waiting deltas in one transaction. Only
/// <see cref="BatchingDatasetUpdateReportingPlugin{TDto,TReporter}"/> honours it; the standard
/// <c>DatasetUpdateReportingPlugin</c> keeps one transaction per delta for every reporter.
/// </summary>
public interface IBatchedDatasetUpdateReporting<T> : IDatasetUpdateReporting<T>
{
    /// <summary>The most deltas the plugin puts into one transaction. At least 1.</summary>
    int MaxDeltasPerTransaction { get; }
}
