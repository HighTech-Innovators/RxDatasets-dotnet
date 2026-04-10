using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Provides read-only access to the current in-memory dataset state.
/// Implemented by DatasetWriter so that SnapshotCreationPlugin can borrow
/// the already-built index instead of parsing the snapshot index again.
/// </summary>
public interface ICurrentIndexProvider<T>
{
    ImmutableDatasetState? CurrentDatasetState { get; }
}
