namespace RxDatasets.Abstractions;

public interface IDatasetWriter<T>
{
    record struct WriteResponse(ulong NewDatasetVersion, uint NumAddedEntities, uint NumUpdatedEntities, uint NumDeletedEntities);
    Task<WriteResponse> Delete(string name, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default);
    Task<WriteResponse> DeleteMany(IAsyncEnumerable<(string name, string? trackingId, DateTimeOffset? slaTimestamp)> names, CancellationToken ct = default);
    Task<WriteResponse> DeleteChildren(string ancestor, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default);
    Task<WriteResponse> Put(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default);
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default);
    /// <summary>
    /// Performs a mix of put and delete operations in a single transactional delta. If <c>entityBytes</c> is null the item is treated as a delete.
    /// Deleting a non-existent (or already deleted) entity is ignored.
    /// Puts that don't change bytes are ignored (like <see cref="PutMany"/> logic).
    /// No implicit deletes are performed (unlike <see cref="PutAll"/> / <see cref="PutAllChildren"/>).
    /// </summary>
    Task<WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> changes, CancellationToken ct = default);

    [Obsolete("Use Delete(name, trackingId, slaTimestamp, ct) instead.")]
    Task<WriteResponse> Delete(string name, CancellationToken ct = default)
        => Delete(name, null, null, ct);

    [Obsolete("Use DeleteChildren(ancestor, trackingId, slaTimestamp, ct) instead.")]
    Task<WriteResponse> DeleteChildren(string ancestor, CancellationToken ct = default)
        => DeleteChildren(ancestor, null, null, ct);

    [Obsolete("Use Put(name, entityBytes, trackingId, slaTimestamp, ct) instead.")]
    Task<WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default)
        => Put(name, entityBytes, null, null, ct);

    [Obsolete("Use DeleteMany with (name, trackingId, slaTimestamp) tuples instead.")]
    Task<WriteResponse> DeleteMany(IAsyncEnumerable<string> names, CancellationToken ct = default)
        => DeleteMany(names.Select(n => (n, (string?)null, (DateTimeOffset?)null)), ct);

    [Obsolete("Use PutAll with (name, entityBytes, trackingId, slaTimestamp) tuples instead.")]
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
        => PutAll(entities.Select(e => (e.name, e.entityBytes, (string?)null, (DateTimeOffset?)null)), ct);

    [Obsolete("Use PutMany with (name, entityBytes, trackingId, slaTimestamp) tuples instead.")]
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
        => PutMany(entities.Select(e => (e.name, e.entityBytes, (string?)null, (DateTimeOffset?)null)), ct);

    [Obsolete("Use PutAllChildren with (name, entityBytes, trackingId, slaTimestamp) tuples instead.")]
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
        => PutAllChildren(entities.Select(e => (e.name, e.entityBytes, (string?)null, (DateTimeOffset?)null)), ct);

    [Obsolete("Use PutOrDeleteMany with (name, entityBytes, trackingId, slaTimestamp) tuples instead.")]
    Task<WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes)> changes, CancellationToken ct = default)
        => PutOrDeleteMany(changes.Select(e => (e.name, e.entityBytes, (string?)null, (DateTimeOffset?)null)), ct);
}
