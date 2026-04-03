namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting<T>
{

    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);
    Task CommitDeltaTransaction(object state) => Task.CompletedTask;
    Task RollBackDeltaTransaction(object state) => Task.CompletedTask;

    Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);

    string ReportingIdentifier { get; }
}
