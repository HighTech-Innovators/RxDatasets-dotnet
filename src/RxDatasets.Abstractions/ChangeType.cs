namespace RxDatasets.Abstractions;

/// <summary>
/// Represents the type of change made to an entity in a write operation.
/// Used in DetailedChange responses to indicate what happened to each entity.
/// </summary>
public enum ChangeType
{
    /// <summary>
    /// Entity was newly created in this operation.
    /// The entity did not exist before this write operation.
    /// </summary>
    New,

    /// <summary>
    /// Entity was modified in this operation.
    /// The entity existed before, and its content changed.
    /// Change detection uses CRC32 comparison: if the new bytes produce a different hash, it's considered CHANGED.
    /// </summary>
    Changed,

    /// <summary>
    /// Entity was deleted in this operation.
    /// The entity existed before and is now removed from the dataset.
    /// </summary>
    Deleted,

    /// <summary>
    /// No change occurred for this entity.
    /// The entity was not affected by the write operation.
    /// Typically used for tracking purposes; not included in DetailedChanges lists.
    /// </summary>
    None
}
