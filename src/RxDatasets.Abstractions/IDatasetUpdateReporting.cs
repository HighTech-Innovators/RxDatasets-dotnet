namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting<T>
{

    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);
    Task CommitDeltaTransaction(object state) => Task.CompletedTask;
    Task RollBackDeltaTransaction(object state) => Task.CompletedTask;

    Task OnNewDto(string name, byte[] newProtoBytes, object? optionalTransaction);
    Task OnChangedDto(string name, byte[] newProtoBytes, object? optionalTransaction);
    Task OnDeletedDto(string name, object? optionalTransaction);

    string ReportingIdentifier { get; }
}
