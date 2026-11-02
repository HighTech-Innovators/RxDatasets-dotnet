using System.Reactive.Concurrency;
using FluentAssertions;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.TestImplementations.InMemoryProvider;
using static RxDatasets.StorageTests.AliasTestHost;

namespace RxDatasets.StorageTests;

/// <summary>
/// Aliases over a restart: a new host, with the same, other or no alias definitions, on the storage an earlier host
/// wrote. The hosts use the shared reader/writer.
/// </summary>
public class AliasHostRestartTests
{
    private const string A = "things/A";
    private const string B = "things/B";
    private const string C = "things/C";
    private const string D = "things/D";
    private const string E = "things/E";
    private const string F = "things/F";

    private readonly LowLevelInMemoryStorage<Demodto> _storage =
        new(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());

    private Task<AliasTestHost> Start(AliasHostOptions? options = null) => AliasTestHost.Start(_storage, options);

    private static async Task<string?> Owner(AliasTestHost host, string alias, string aliasName = ByValue) =>
        (await host.Alias(aliasName).Get(alias))?.name;

    [Fact]
    public async Task ARestartWithTheSameDefinitions_ServesTheAliasesAtOnce_WithoutARebuild()
    {
        ulong version;
        await using (var host = await Start())
        {
            await host.Writer.PutMany(Puts((A, "x"), (B, "y")));
            version = (await host.Writer.Delete(B, null, null)).NewDatasetVersion;
        }

        await using var restarted = await Start();

        (await restarted.NewestDelta()).Should().Be(version, "the same definitions need no rebuild");
        (await Owner(restarted, "values/x")).Should().Be(A);
        (await Owner(restarted, "values/y")).Should().BeNull();
        var conflicting = () => restarted.Writer.Put(C, Entity(C, "x"), null, null);
        (await conflicting.Should().ThrowAsync<AliasConflictException>()).Which.OwnerName.Should().Be(A);
    }

    [Fact]
    public async Task ASnapshotMadeByTheHost_CarriesTheAliasesOverARestart()
    {
        // The first delta gets a snapshot at once, the next after every 3 deltas
        AliasHostOptions options = new() { Snapshots = true, Configuration = { ["SnapshotCreation:SnapshotInterval"] = "3" } };
        ulong version;
        await using (var host = await Start(options))
        {
            await host.Writer.PutMany(Puts((A, "x"), (B, "y")));
            await host.WaitForSnapshot(1);
            await host.Writer.Delete(B, null, null);
            await host.Writer.Put(C, Entity(C, "y"), null, null);
            await host.Writer.Put(D, Entity(D, "z"), null, null);
            await host.WaitForSnapshot(4);
            version = (await host.Writer.Put(E, Entity(E, "e"), null, null)).NewDatasetVersion;
        }

        await using var restarted = await Start(options);

        restarted.Services.GetRequiredService<DatasetReaderWriter<Demodto>>().GetMainIndexSnapshotVersion()
            .Should().Be(4, "the index starts from the snapshot");
        (await restarted.NewestDelta()).Should().Be(version, "the hash in the snapshot matches, so there is no rebuild");
        await ShouldServeTheAliasesOfAToE(restarted);
    }

    [Fact]
    public async Task AfterCleanupDeletedTheOldDeltas_ARestart_StillHasTheAliases()
    {
        // Cleanup only deletes what is older than 2 hours, so the hosts run 3 hours ahead
        AliasHostOptions options = new()
        {
            Snapshots = true,
            Cleanup = true,
            Configuration =
            {
                ["SnapshotCreation:SnapshotInterval"] = "1",
                ["Cleanup:KeepAllDeltas"] = "false",
                ["Cleanup:MinimumSnapshotsToKeep"] = "2"
            },
            ConfigureServices = services => services.AddSingleton<IScheduler>(new ShiftedScheduler(TimeSpan.FromHours(3)))
        };
        ulong version;
        await using (var host = await Start(options))
        {
            await host.Writer.PutMany(Puts((A, "x"), (B, "y")));
            await host.WaitForSnapshot(1);
            await host.Writer.Delete(B, null, null);
            await host.WaitForSnapshot(2);
            await host.Writer.Put(C, Entity(C, "y"), null, null);
            await host.WaitForSnapshot(3);
            await host.Writer.Put(D, Entity(D, "z"), null, null);
            await host.WaitForSnapshot(4);
            version = (await host.Writer.Put(E, Entity(E, "e"), null, null)).NewDatasetVersion;
            await host.WaitForSnapshot(5);

            // Cleanup keeps what a consumer hasn't processed, and only a reporting plugin records that; this one is done
            var storageProvider = host.Services.GetRequiredService<IStorageProvider<Demodto>>();
            await storageProvider.StoreProcessedVersion("caught-up-consumer", version);
            await Eventually(async () => (await storageProvider.ListVersions()).OldestDelta > 1,
                "cleanup deletes the oldest deltas");
        }

        await using var restarted = await Start(options);

        (await restarted.NewestDelta()).Should().Be(version);
        await ShouldServeTheAliasesOfAToE(restarted);
    }

    /// <summary>The aliases after: A x, B y; delete B; C y; D z; E e.</summary>
    private static async Task ShouldServeTheAliasesOfAToE(AliasTestHost host)
    {
        (await Owner(host, "values/x")).Should().Be(A);
        (await Owner(host, "values/y")).Should().Be(C, "B released it before C claimed it");
        (await Owner(host, "values/z")).Should().Be(D);
        (await Owner(host, "values/e")).Should().Be(E);
        (await Owner(host, "values/B", ByThing)).Should().BeNull("B is deleted");
        var conflicting = () => host.Writer.Put(F, Entity(F, "y"), null, null);
        (await conflicting.Should().ThrowAsync<AliasConflictException>()).Which.OwnerName.Should().Be(C);
    }

    /// <summary><see cref="Scheduler.Default"/>, with its clock <paramref name="shift"/> ahead.</summary>
    private sealed class ShiftedScheduler(TimeSpan shift) : IScheduler
    {
        private static IScheduler Inner => Scheduler.Default;

        public DateTimeOffset Now => Inner.Now + shift;

        public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
            Inner.Schedule(state, action);

        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
            Inner.Schedule(state, dueTime, action);

        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime,
            Func<IScheduler, TState, IDisposable> action) =>
            Inner.Schedule(state, dueTime - shift, action);
    }
}
