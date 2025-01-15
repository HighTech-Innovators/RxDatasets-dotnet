namespace RxDatasets.Abstractions;

public interface IDatasetWriter<T>
{
    public record struct WriteResponse(ulong NewDatasetVersion, uint NumAddedEntities, uint NumUpdatedEntities, uint NumDeletedEntities);
    Task<WriteResponse> Delete(string name, CancellationToken ct = default);
    Task<WriteResponse> DeleteMany(string ancestor, CancellationToken ct = default);
    Task<WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default);
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default);
}
