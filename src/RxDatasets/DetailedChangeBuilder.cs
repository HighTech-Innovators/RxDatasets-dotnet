using System.Collections.Generic;

namespace RxDatasets;

/// <summary>
/// Helper utilities for creating and managing detailed change tracking.
/// Used by gRPC service layer to populate detailed_changes in WriteResponse when X-Debug header is present.
/// </summary>
public static class DetailedChangeBuilder
{
    /// <summary>
    /// Placeholder utility for future detailed change tracking integration.
    /// The actual change type determination is handled in the WriterServiceImpl mapping layer.
    /// </summary>
    public static string FormatEntityChange(string entityId)
    {
        return entityId;
    }
}


