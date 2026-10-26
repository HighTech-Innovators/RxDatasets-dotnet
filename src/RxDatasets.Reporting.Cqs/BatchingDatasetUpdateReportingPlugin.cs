using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting.Cqs;

/// <summary>
/// Replays the deltas a reporter has not seen yet, like <c>DatasetUpdateReportingPlugin</c>, but puts up
/// to <see cref="IBatchedDatasetUpdateReporting{T}.MaxDeltasPerTransaction"/> consecutive deltas into one
/// transaction: one <c>OpenDeltaTransaction</c>, every delta's entities, one <c>CommitDeltaTransaction</c>,
/// and one write of the processed-version cursor, which then covers the whole transaction.
/// </summary>
/// <remarks>
/// This is a separate class so that the standard plugin, and every reporter written for it, stays exactly
/// as it was. With a limit of 1 this plugin makes the same reporter calls as the standard one, except that
/// a delta without an index file is skipped inside a transaction that is still opened and committed.
/// A reporter that cannot take that must not use this plugin.
/// </remarks>
public class BatchingDatasetUpdateReportingPlugin<TDto, TReporter>(
    IStorageProvider<TDto> storageProvider,
    ILogger<BatchingDatasetUpdateReportingPlugin<TDto, TReporter>> logger,
    TReporter reportingImplementation)
    : IDatasetUpdateConsumer<TDto> where TReporter : IBatchedDatasetUpdateReporting<TDto>
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    public string ConsumerIdentifier { get; } = $"DatasetUpdatePlugin-of-{reportingImplementation.ReportingIdentifier}";

    private string ReportingIdentifier => reportingImplementation.ReportingIdentifier;

    public async Task HandleNewDeltas(CancellationToken stoppingToken)
    {
        int limit = reportingImplementation.MaxDeltasPerTransaction;
        if (limit <= 0)
        {
            throw new InvalidOperationException("MaxDeltasPerTransaction must be positive.");
        }

        ulong newestDelta = (await storageProvider.ListVersions()).NewestDelta;
        ulong processed = await storageProvider.GetProcessedVersion(ReportingIdentifier);

        // Set when a delta's data files are missing in the middle of a transaction: the deltas before it are
        // reported first, so the broken one starts a transaction of its own and can be skipped whole.
        ulong? brokenDelta = null;

        while (!stoppingToken.IsCancellationRequested && processed < newestDelta)
        {
            ulong first = processed + 1;
            ulong last = processed + Math.Min((ulong)limit, newestDelta - processed);
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
                logger.LogInformation("Processed deltas {First} to {Last}", first, last);
            }
            catch (DeltaDataMissingException missing)
            {
                if (missing.Version == first)
                {
                    // Nothing of it was reported (the transaction is rolled back), so it is skipped, as the
                    // standard plugin does.
                    await storageProvider.StoreProcessedVersion(ReportingIdentifier, first);
                    processed = first;
                    brokenDelta = null;
                }
                else
                {
                    brokenDelta = missing.Version;
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning(e, "Something went wrong while processing deltas {First} to {Last}; retrying", first, last);
                await Task.Delay(RetryDelay, stoppingToken);
            }
        }
    }

    /// <summary>One transaction over <paramref name="first"/> to <paramref name="last"/>; rolled back unless committed.</summary>
    private async Task ReportTransaction(ulong first, ulong last, CancellationToken cancellationToken)
    {
        object? state = await reportingImplementation.OpenDeltaTransaction(first);
        try
        {
            for (ulong version = first; version <= last; version++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await ReportDelta(version, state, cancellationToken);
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
                logger.LogInformation("Rolling back the transaction that starts at delta {Version}", first);
                await reportingImplementation.RollBackDeltaTransaction(state);
            }
        }
    }

    private async Task ReportDelta(ulong version, object? state, CancellationToken cancellationToken)
    {
        DeltaIndex? index = await ReadIndex(version);
        if (index == null)
        {
            return;
        }

        logger.LogDebug("Processing delta {Version}", version);
        try
        {
            // The entities come from the tree a writer stores and from the legacy flat lists of older deltas, with
            // their full names: in the tree an entity's own name is empty. The order is the standard plugin's.
            foreach (var (name, entity) in index.EnumerateNewEntities())
            {
                cancellationToken.ThrowIfCancellationRequested();
                await reportingImplementation.OnNewDto(name, await ReadData(version, entity),
                    NullIfEmpty(entity.TrackingId), entity.SlaTimestamp?.ToDateTimeOffset(), state);
                await ReportAliasChanges(name, entity.AddedAliases, entity.RemovedAliases, entity.TrackingId,
                    entity.SlaTimestamp, state);
            }

            foreach (var (name, entity) in index.EnumerateChangedEntities())
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

            foreach (var (name, entity) in index.EnumerateDeletedEntities())
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

    /// <summary>Lost aliases first, then gained ones, as the standard plugin reports them.</summary>
    private async Task ReportAliasChanges(string entityName, IEnumerable<AliasRef> added, IEnumerable<AliasRef> removed,
        string trackingId, Timestamp? slaTimestamp, object? state)
    {
        var changes = removed.Select(alias => (alias, ChangeType.Deleted))
            .Concat(added.Select(alias => (alias, ChangeType.New)));
        foreach (var (alias, change) in changes)
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

    private async Task<byte[]> ReadData(ulong version, DeltaEntity entity) =>
        (await storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, entity.Position)], entity.Length))
        .ToArray();

    private static string? NullIfEmpty(string value) => string.IsNullOrEmpty(value) ? null : value;

    private sealed class DeltaDataMissingException(ulong version) : Exception
    {
        public ulong Version { get; } = version;
    }
}
