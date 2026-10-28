namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting<T>
{

    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);

    /// <summary>
    /// What the reporting plugin calls to open a transaction. <paramref name="cancellationToken"/> is the plugin's stopping
    /// token: a reporter can cancel the transaction's outstanding work with it, so a shutdown does not wait for that work
    /// to finish. By default it opens the transaction with <see cref="OpenDeltaTransaction(ulong)"/>.
    /// </summary>
    Task<object?> OpenDeltaTransaction(ulong deltaVersion, CancellationToken cancellationToken) =>
        OpenDeltaTransaction(deltaVersion);

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

    /// <summary>
    /// Whether <see cref="OnNewDto"/> and <see cref="OnChangedDto"/> do without the entity's bytes. When true, the
    /// reporting plugin does not read the delta's data file and passes an empty array: a reporter that only needs the names
    /// then costs no read per entity, and a delta whose data file is missing is still reported from its index. False, the
    /// default, is also what a mock that does not run default interface members gets.
    /// </summary>
    bool SkipsDtoBytes => false;

    /// <summary>
    /// Whether a reporter without a processed version yet starts at the newest delta and reports only what comes after
    /// it (true), instead of reporting every delta the dataset still keeps, from the oldest one (false, the default, also
    /// what a mock that does not run default interface members gets).
    /// </summary>
    bool SkipsRetainedHistory => false;
}
