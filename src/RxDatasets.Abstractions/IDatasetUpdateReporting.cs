namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting<T>
{
    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);
    Task CommitVersionTransaction(object state);
    Task RollBack(object state); 

    Task OnNewDto(string name, byte[] newProtoBytes, object? optionalTransaction);
    Task OnChangedDto(string name, byte[] newProtoBytes, object? optionalTransaction);
    Task OnDeletedDto(string name, object? optionalTransaction);
}