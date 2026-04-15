using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Provides read-only access to the current in-memory dataset state.
/// Implemented by DatasetWriter so that SnapshotCreationPlugin can borrow
/// the already-built indices instead of parsing the snapshot index again.
/// </summary>
public interface ICurrentIndexProvider<T>
{
    /// <summary>
    /// The full current state: latest snapshot + all deltas applied.
    /// Tells us which entities are alive and their latest version.
    /// </summary>
    ImmutableDatasetState? CurrentDatasetState { get; }

    /// <summary>
    /// The most recent snapshot-only state. May be newer than
    /// <see cref="CurrentDatasetState"/>'s SnapshotVersion when the writer
    /// has received a snapshot announcement but hasn't reloaded the main index.
    /// Entity positions reference this snapshot's data file.
    /// </summary>
    ImmutableDatasetState? MostRecentSnapshotState { get; }
}
