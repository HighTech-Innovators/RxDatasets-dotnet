namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting<T>
{

    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);
    Task CommitDeltaTransaction(object state) => Task.CompletedTask;
    Task RollBackDeltaTransaction(object state) => Task.CompletedTask;

    Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);

    /// <summary>
    /// An alias was gained (<see cref="ChangeType.New"/>) or lost (<see cref="ChangeType.Deleted"/>) by
    /// <paramref name="entityName"/>, also when the entity was deleted.
    /// </summary>
    Task OnAliasChanged(string aliasName, string alias, string entityName, ChangeType change, string? trackingId,
        DateTimeOffset? slaTimestamp, object? optionalTransaction) => Task.CompletedTask;

    string ReportingIdentifier { get; }
}
