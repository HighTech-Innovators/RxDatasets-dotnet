using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.BackgroundServices;

/// <summary>
/// Reports the deltas a reporter has not seen yet, in order, and writes the reporter's processed version after each
/// transaction. A reporter that implements <see cref="IBatchedDatasetUpdateReporting{T}"/> gets up to
/// <see cref="IBatchedDatasetUpdateReporting{T}.MaxDeltasPerTransaction"/> consecutive deltas in one transaction: one
/// <c>OpenDeltaTransaction</c>, every delta's entities, one <c>CommitDeltaTransaction</c> and one cursor write. Any other
/// reporter gets one transaction per delta.
/// </summary>
/// <remarks>
/// A transaction is opened at the first delta in its range that has an index file, so a delta without one opens no
/// transaction of its own and is skipped. A delta whose data file is missing is rolled back and skipped: the deltas
/// before it in the same transaction are reported again without it, in a transaction of their own.
/// </remarks>
/// <param name="firstRetryDelay">
/// How long a failed transaction waits before it is tried again, 5 seconds by default. The wait doubles on each failure
/// in a row, up to a minute.
/// </param>
/// <exception cref="ArgumentOutOfRangeException">
/// The reporter's <see cref="IBatchedDatasetUpdateReporting{T}.MaxDeltasPerTransaction"/> is not positive.
/// </exception>
public class DatasetUpdateReportingPlugin<TDto, TReporter>(
    IStorageProvider<TDto> storageProvider,
    ILogger<DatasetUpdateReportingPlugin<TDto, TReporter>> logger,
    TReporter reportingImplementation,
    TimeSpan? firstRetryDelay = null)
    : IDatasetUpdateConsumer<TDto> where TReporter : IDatasetUpdateReporting<TDto>
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(1);

    private readonly TimeSpan _firstRetryDelay = firstRetryDelay ?? TimeSpan.FromSeconds(5);

    // Checked here rather than in HandleNewDeltas, where the consuming service would retry it forever
    private readonly int _transactionLimit = TransactionLimit(reportingImplementation);

    // Set once this process has decided where a reporter without a processed version starts; see ReadProgress
    private bool _startChosen;

    public string ConsumerIdentifier { get; } = $"DatasetUpdatePlugin-of-{reportingImplementation.ReportingIdentifier}";

    private string ReportingIdentifier => reportingImplementation.ReportingIdentifier;

    public async Task HandleNewDeltas(CancellationToken stoppingToken)
    {
        (ulong newestDelta, ulong processed) = await ReadProgress();

        // Set when a delta's data files are missing in the middle of a transaction: the deltas before it are
        // reported first, so the broken one starts a transaction of its own and can be skipped whole.
        ulong? brokenDelta = null;
        TimeSpan retryDelay = _firstRetryDelay;

        while (!stoppingToken.IsCancellationRequested && processed < newestDelta)
        {
            ulong first = processed + 1;
            ulong last = processed + Math.Min((ulong)_transactionLimit, newestDelta - processed);
            if (brokenDelta is { } broken)
            {
                last = Math.Min(last, broken - 1);
            }

            try
            {
                await ReportTransaction(first, last, stoppingToken);
                await storageProvider.StoreProcessedVersion(ReportingIdentifier, last);
                processed = last;
                brokenDelta = null;
                retryDelay = _firstRetryDelay;
                logger.LogInformation("Processed deltas {First} to {Last}", first, last);
            }
            catch (DeltaDataMissingException missing)
            {
                if (missing.Version == first)
                {
                    // Skipped. Rolling back does not unsend what a reporter already sent: CqsReporter pushes every
                    // MaxNotificationsPerPush notifications, so the part of this delta before the missing data file may
                    // already be in CQS. Those notifications stay delivered, which at-least-once allows.
                    await storageProvider.StoreProcessedVersion(ReportingIdentifier, first);
                    processed = first;
                    brokenDelta = null;
                }
                else
                {
                    brokenDelta = missing.Version;
                }
            }
            // A cancellation that is not the stopping token's, such as an HTTP timeout from a storage SDK, is a failure
            // like any other: thrown on, it would end the consuming loop and stop the host.
            catch (Exception e) when (e is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
            {
                logger.LogWarning(e, "Something went wrong while processing deltas {First} to {Last}; retrying in {Delay}",
                    first, last, retryDelay);
                await Task.Delay(retryDelay, stoppingToken);
                retryDelay = TimeSpan.FromTicks(Math.Min(retryDelay.Ticks * 2, MaxRetryDelay.Ticks));
                // Read again rather than retry the same range: if the lock was lost, another instance may have reported
                // and moved the cursor on, and writing this range's end would move it back. While the lock is lost the
                // read throws, which ends this run; the consuming service starts the next once storage is ready again.
                (newestDelta, processed) = await ReadProgress();
                brokenDelta = null;
            }
        }
    }

    private static int TransactionLimit(TReporter reporter)
    {
        int limit = reporter is IBatchedDatasetUpdateReporting<TDto> batched ? batched.MaxDeltasPerTransaction : 1;
        return limit > 0
            ? limit
            : throw new ArgumentOutOfRangeException(nameof(reporter), limit,
                $"{reporter.ReportingIdentifier}: MaxDeltasPerTransaction must be positive.");
    }

    /// <summary>
    /// The newest delta, and the last delta this reporter has handled. That is its processed version, except for two cases.
    /// A reporter without one that skips the retained history starts at the newest delta: that is written as its processed
    /// version, once per process, so the first delta of a dataset that was still empty is not skipped later. And deltas
    /// before the oldest one the dataset keeps have been cleaned up, so the reporter starts after them.
    /// </summary>
    private async Task<(ulong NewestDelta, ulong Processed)> ReadProgress()
    {
        (ulong oldestDelta, ulong newestDelta, _) = await storageProvider.ListVersions();
        ulong processed = await storageProvider.GetProcessedVersion(ReportingIdentifier);
        if (!_startChosen && processed == 0 && newestDelta > 0 && reportingImplementation.SkipsRetainedHistory)
        {
            await storageProvider.StoreProcessedVersion(ReportingIdentifier, newestDelta);
            logger.LogInformation("{Reporter} starts at delta {Newest}: the {Count} deltas before it are not reported",
                ReportingIdentifier, newestDelta, newestDelta - (oldestDelta == 0 ? 0 : oldestDelta - 1));
            processed = newestDelta;
        }

        _startChosen = true;
        ulong cleanedUp = oldestDelta == 0 ? 0 : oldestDelta - 1;
        if (processed < cleanedUp)
        {
            if (processed > 0)
            {
                logger.LogWarning("Deltas {First} to {Last} were cleaned up before {Reporter} reported them",
                    processed + 1, cleanedUp, ReportingIdentifier);
            }

            processed = cleanedUp;
        }

        return (newestDelta, processed);
    }

    /// <summary>One transaction over <paramref name="first"/> to <paramref name="last"/>; rolled back unless committed.</summary>
    private async Task ReportTransaction(ulong first, ulong last, CancellationToken cancellationToken)
    {
        bool opened = false;
        object? state = null;
        try
        {
            for (ulong version = first; version <= last; version++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                DeltaIndex? index = await ReadIndex(version);
                if (index == null)
                {
                    continue;
                }

                if (!opened)
                {
                    state = await reportingImplementation.OpenDeltaTransaction(version, cancellationToken);
                    opened = true;
                }

                await ReportDelta(version, index, state, cancellationToken);
            }

            if (state != null)
            {
                await reportingImplementation.CommitDeltaTransaction(state);
                state = null; // committed: nothing to roll back
            }
        }
        finally
        {
            if (state != null)
            {
                logger.LogInformation("Rolling back the transaction over deltas {First} to {Last}", first, last);
                await reportingImplementation.RollBackDeltaTransaction(state);
            }
        }
    }

    private async Task ReportDelta(ulong version, DeltaIndex index, object? state, CancellationToken cancellationToken)
    {
        logger.LogDebug("Processing delta {Version}", version);
        try
        {
            // The entities come from the tree a writer stores and from the legacy flat lists of older deltas, with
            // their full names: in the tree an entity's own name is empty.
            foreach ((string name, DeltaEntity entity) in index.EnumerateNewEntities())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await reportingImplementation.OnNewDto(name, await ReadData(version, entity),
                    NullIfEmpty(entity.TrackingId), entity.SlaTimestamp?.ToDateTimeOffset(), state);
                await ReportAliasChanges(name, entity.AddedAliases, entity.RemovedAliases, entity.TrackingId,
                    entity.SlaTimestamp, state);
            }

            foreach ((string name, DeltaEntity entity) in index.EnumerateChangedEntities())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await reportingImplementation.OnChangedDto(name, await ReadData(version, entity),
                    NullIfEmpty(entity.TrackingId), entity.SlaTimestamp?.ToDateTimeOffset(), state);
                await ReportAliasChanges(name, entity.AddedAliases, entity.RemovedAliases, entity.TrackingId,
                    entity.SlaTimestamp, state);
            }

            foreach (string name in index.DeletedNames)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await reportingImplementation.OnDeletedDto(name, null, null, state);
            }

            foreach ((string name, DeletedEntity entity) in index.EnumerateDeletedEntities())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await reportingImplementation.OnDeletedDto(name, NullIfEmpty(entity.TrackingId),
                    entity.SlaTimestamp?.ToDateTimeOffset(), state);
                await ReportAliasChanges(name, [], entity.RemovedAliases, entity.TrackingId, entity.SlaTimestamp, state);
            }

            // Alias changes without a data change, written by a rebuild after the alias definitions changed.
            foreach (AliasOnlyChange change in index.AliasChanges)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await ReportAliasChanges(change.Name, change.AddedAliases, change.RemovedAliases, "", null, state);
            }
        }
        catch (FileNotFoundException)
        {
            logger.LogWarning(
                "Data files for delta {Version} don't exist. Either data is corrupted or is deleted. Skipping.", version);
            throw new DeltaDataMissingException(version);
        }
    }

    /// <summary>Lost aliases first, then gained ones.</summary>
    private async Task ReportAliasChanges(string entityName, IEnumerable<AliasRef> added, IEnumerable<AliasRef> removed,
        string trackingId, Timestamp? slaTimestamp, object? state)
    {
        IEnumerable<(AliasRef Alias, ChangeType Change)> changes = removed.Select(alias => (alias, ChangeType.Deleted))
            .Concat(added.Select(alias => (alias, ChangeType.New)));
        foreach ((AliasRef alias, ChangeType change) in changes)
        {
            await reportingImplementation.OnAliasChanged(alias.Name, alias.Value, entityName, change,
                NullIfEmpty(trackingId), slaTimestamp?.ToDateTimeOffset(), state);
        }
    }

    /// <summary>The delta's index, or null when its file is missing (the delta is then skipped).</summary>
    private async Task<DeltaIndex?> ReadIndex(ulong version)
    {
        try
        {
            ReadOnlyMemory<byte> bytes = await storageProvider.ReadDeltaIndex(version);
            DeltaIndex index = DeltaIndex.Parser.ParseFrom(bytes.Span);
            index.ValidateAgainstVersion(version);
            return index;
        }
        catch (FileNotFoundException)
        {
            logger.LogWarning("Index file for delta {Version} doesn't exist. Skipping.", version);
            return null;
        }
    }

    // Empty, without reading the data file, for a reporter that does not use the bytes
    private async Task<byte[]> ReadData(ulong version, DeltaEntity entity) =>
        reportingImplementation.SkipsDtoBytes
            ? []
            : (await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, entity.Position)],
                entity.Length)).ToArray();

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;

    private sealed class DeltaDataMissingException(ulong version) : Exception
    {
        public ulong Version { get; } = version;
    }
}
