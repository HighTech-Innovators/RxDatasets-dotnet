namespace RxDatasets.Abstractions;

public interface IDatasetWriter<T>
{
    record struct WriteResponse(ulong NewDatasetVersion, uint NumAddedEntities, uint NumUpdatedEntities, uint NumDeletedEntities);
    Task<WriteResponse> Delete(string name, CancellationToken ct = default);
    Task<WriteResponse> DeleteMany(IAsyncEnumerable<string> names, CancellationToken ct = default);
    Task<WriteResponse> DeleteChildren(string ancestor, CancellationToken ct = default);
    Task<WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default);
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
    /// <summary>
    /// Performs a mix of put and delete operations in a single transactional delta. If <c>entityBytes</c> is null the item is treated as a delete.
    /// Deleting a non-existent (or already deleted) entity is ignored.
    /// Puts that don't change bytes are ignored (like <see cref="PutMany"/> logic).
    /// No implicit deletes are performed (unlike <see cref="PutAll"/> / <see cref="PutAllChildren"/>).
    /// </summary>
    Task<WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes)> changes, CancellationToken ct = default);
}
