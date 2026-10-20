using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// The reader started as a hosted service re-reads its index from storage every five seconds; that must not
/// put back an older index than an update notification already brought.
/// </summary>
public class DatasetReaderReinitializationTests
{
    // Two of the reader's five-second re-initialisations, with time to spare.
    private static readonly TimeSpan RunFor = TimeSpan.FromSeconds(11);

    [Fact]
    public async Task Should_never_report_an_older_version_than_it_did_before_When_it_reinitialises_during_updates()
    {
        // Given — a reader with its periodic re-initialisation, and a writer on the same storage
        using var hub = new NotificationsHubSync<object>();
        var storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, hub);
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, storage, hub);
        await reader.StartAsync(CancellationToken.None);
        var writer = new DatasetWriter<object>(
            hub, NullLogger<DatasetWriter<object>>.Instance, storage, Options.Create(new WriterConfig()), false);
        using var stop = new CancellationTokenSource(RunFor);

        // When — writing all the time, and watching the version the reader reports
        var writes = Task.Run(async () =>
        {
            var i = 0;
            while (!stop.IsCancellationRequested)
            {
                await writer.Put($"entity-{i++}", [1, (byte)i], null, null);
            }
        });
        var regressions = new List<(ulong Seen, ulong Then)>();
        var highest = reader.GetLatestVersion();
        var watch = Stopwatch.StartNew();
        while (watch.Elapsed < RunFor)
        {
            var now = reader.GetLatestVersion();
            if (now < highest)
            {
                regressions.Add((highest, now));
            }

            highest = Math.Max(highest, now);
        }

        await writes;
        await reader.StopAsync(CancellationToken.None);

        // Then — the version only ever moved forward
        highest.Should().BeGreaterThan(0, "the writer wrote");
        regressions.Should().BeEmpty("a re-initialisation must not undo an update that was already applied");
    }
}
