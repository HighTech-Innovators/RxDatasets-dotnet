using System.Diagnostics.Metrics;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

/// <summary>
/// Aliases checked by the writer and kept in the index, with the alias shapes that can really conflict:
/// <c>by_storeesl</c> (one link per ESL), <c>by_sic</c> (many per item) and <c>by_externalid</c> (one store per id).
/// </summary>
/// <remarks>
/// An entity's payload is text like <c>by_storeesl=t/T/s/S/esls/B1/link;by_sic=a</c>; a definition takes the values
/// of its alias name, and throws on a payload that starts with '!'.
/// </remarks>
public class FirstClassAliasTests
{
    private const string StoreEsl = "by_storeesl";
    private const string Sic = "by_sic";
    private const string ExternalId = "by_externalid";
    private const string L1 = "t/T/s/S/links/L1";
    private const string L2 = "t/T/s/S/links/L2";
    private const string L3 = "t/T/s/S/links/L3";
    private const string L4 = "t/T/s/S/links/L4";
    private const string B1 = "t/T/s/S/esls/B1/link";
    private const string B2 = "t/T/s/S/esls/B2/link";
    private const string B3 = "t/T/s/S/esls/B3/link";

    private static readonly AliasDefinition[] Definitions = [Definition(StoreEsl), Definition(Sic)];

    private readonly IOptions<StorageConfig> _storageConfig = Options.Create(new StorageConfig());
    private readonly CountingStorage _storage =
        new(new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>()));
    private INotificationsHub<object> _hub = null!;
    private ObjectStorageProvider<object> _provider = null!;

    private static AliasDefinition Definition(string aliasName, string key = "", string hash = "v1") =>
        new(aliasName, (_, bytes) =>
        {
            var text = Encoding.UTF8.GetString(bytes.Span);
            if (text.StartsWith('!'))
                throw new FormatException("Not a payload");
            return text.Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Select(pair => pair.Split('='))
                .Where(pair => pair[0] == (key == "" ? aliasName : key))
                .Select(pair => pair[1]);
        }, hash);

    private static byte[] Payload(params string[] aliases) => Encoding.UTF8.GetBytes(string.Join(';', aliases));

    private static string Esl(string alias) => $"{StoreEsl}={alias}";

    /// <summary>Starts a writer, as after a (re)start, on the storage the earlier ones wrote.</summary>
    /// <param name="rebuilds">
    /// Whether the start rebuilds the aliases. The rebuild stores a delta, and the storage provider takes writes only
    /// once it is ready; with the synchronous hub it is ready only after the writer, so such a start uses the
    /// asynchronous hub of a real host.
    /// </param>
    /// <param name="initializes">Whether to wait for the writer to initialize.</param>
    /// <param name="retryInitialization">Whether the writer retries a failed initialization.</param>
    private async Task<DatasetReaderWriter<object>> StartWriter(AliasDefinition[]? definitions = null, bool coalescing = false,
        bool rebuilds = false, bool initializes = true, bool retryInitialization = true)
    {
        _hub = rebuilds ? new NotificationsHub<object>() : new NotificationsHubSync<object>();
        var storageReader =
            new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, _storage);
        _provider = new ObjectStorageProvider<object>(Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig,
            _storage, _hub, storageReader, false);
        var aliasOptions = new AliasOptions<object>();
        aliasOptions.Definitions.AddRange(definitions ?? Definitions);
        var writer = new DatasetReaderWriter<object>(_hub, Mock.Of<ILogger<DatasetWriter<object>>>(), _provider,
            Options.Create(new WriterConfig { EnableWriteCoalescing = coalescing }), Options.Create(aliasOptions),
            retryInitialization);

        await _provider.StartAsync(CancellationToken.None);
        while (writer.InitializationTask == null)
            await Task.Delay(10);
        if (initializes)
            await writer.InitializationTask.WaitAsync(TimeSpan.FromSeconds(10));
        return writer;
    }

    private async Task<DeltaIndex> ReadDelta(ulong version) =>
        DeltaIndex.Parser.ParseFrom((await _provider.ReadDeltaIndex(version)).Span);

    private static IAsyncEnumerable<(string, byte[]?, string?, DateTimeOffset?)> Changes(
        params (string Name, byte[]? Bytes)[] changes) =>
        changes.Select(c => (c.Name, c.Bytes, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable();

    private static IAsyncEnumerable<(string, byte[], string?, DateTimeOffset?)> Puts(params (string Name, byte[] Bytes)[] puts) =>
        puts.Select(p => (p.Name, p.Bytes, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable();

    [Fact]
    public async Task TwoEntitiesClaimingOneAlias_IsRejected_AndNothingIsStored()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        var version = writer.GetLatestVersion();

        var claimOwned = () => writer.Put(L2, Payload(Esl(B1)), null, null);
        var claimTwiceInOneRequest = () => writer.PutMany(Puts((L3, Payload(Esl(B3))), (L4, Payload(Esl(B3)))));

        (await claimOwned.Should().ThrowAsync<AliasConflictException>()).Which.Should()
            .BeEquivalentTo(new { AliasName = StoreEsl, Alias = B1, EntityName = L2, OwnerName = L1 });
        await claimTwiceInOneRequest.Should().ThrowAsync<AliasConflictException>();
        writer.GetLatestVersion().Should().Be(version);
        (await _provider.ListVersions()).NewestDelta.Should().Be(version, "nothing was stored");
        (await writer.Get(L2)).Should().BeNull();
        (await writer.Get(L3)).Should().BeNull();
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        writer.ResolveAlias(StoreEsl, B3).Should().BeNull();
    }

    [Fact]
    public async Task AnAliasIsUniquePerAliasName_AndOneEntityMayOwnMany()
    {
        var writer = await StartWriter();

        await writer.PutMany(Puts(
            (L1, Payload(Esl(B1), $"{Sic}=1234", $"{Sic}=5678")),
            (L2, Payload(Esl(B2), $"{Sic}={B1}"))));

        writer.ResolveAlias(Sic, "1234").Should().Be(L1);
        writer.ResolveAlias(Sic, "5678").Should().Be(L1);
        writer.ResolveAlias(Sic, B1).Should().Be(L2, "the same value under another alias name is another alias");
        writer.ListAliases(Sic, "").Aliases.Should().BeEquivalentTo("1234", "5678", B1);
    }

    [Fact]
    public async Task FreeingAnAliasEarlierInTheRequest_LetsALaterOperationClaimIt()
    {
        var writer = await StartWriter();
        await writer.PutMany(Puts((L1, Payload(Esl(B1))), (L3, Payload(Esl(B3)))));

        // Deleted, or changed to have no alias, before the claim
        await writer.PutOrDeleteMany(Changes((L1, null), (L2, Payload(Esl(B1)))));
        await writer.PutOrDeleteMany(Changes((L3, Payload()), (L4, Payload(Esl(B3)))));

        writer.ResolveAlias(StoreEsl, B1).Should().Be(L2);
        writer.ResolveAlias(StoreEsl, B3).Should().Be(L4);
        writer.GetEntityAliases(L3).Should().BeEmpty();
    }

    [Fact]
    public async Task FreeingAnAliasLaterInTheRequest_DoesNotLetAnEarlierOperationClaimIt()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);

        var claimThenFree = () => writer.PutOrDeleteMany(Changes((L2, Payload(Esl(B1))), (L1, null)));

        await claimThenFree.Should().ThrowAsync<AliasConflictException>();
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        (await writer.Get(L1)).Should().NotBeNull();
    }

    [Fact]
    public async Task Delete_ReleasesTheAliases_InTheDelta_AndForALaterClaim()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1), $"{Sic}=1234"), null, null);

        var deleted = await writer.Delete(L1, null, null);
        await writer.Put(L2, Payload(Esl(B1)), null, null);

        var delta = await ReadDelta(deleted.NewDatasetVersion);
        delta.EnumerateDeletedEntities().Single().Entity.RemovedAliases.Select(a => (a.Name, a.Value))
            .Should().BeEquivalentTo([(StoreEsl, B1), (Sic, "1234")]);
        delta.AliasDefinitionsHash.Should().Be(AliasOptions<object>.HashOf(Definitions));
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L2);
        writer.ResolveAlias(Sic, "1234").Should().BeNull();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeleteChildren_ReleasesTheAliasesOfTheEntitiesItDeletes(bool coalescing)
    {
        const string otherStoresLink = "t/T/s/X/links/L9";
        var writer = await StartWriter(coalescing: coalescing);
        await writer.PutMany(Puts((L1, Payload(Esl(B1))), (L2, Payload(Esl(B2))), (otherStoresLink, Payload(Esl(B3)))));

        await writer.DeleteChildren("t/T/s/S", null, null);
        await writer.Put(L3, Payload(Esl(B1)), null, null);

        writer.ResolveAlias(StoreEsl, B1).Should().Be(L3);
        writer.ResolveAlias(StoreEsl, B2).Should().BeNull();
        writer.ResolveAlias(StoreEsl, B3).Should().Be(otherStoresLink);
    }

    [Fact]
    public async Task PutAll_MayGiveTheAliasOfAnEntityItImplicitlyDeletes_ToAnother()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);

        await writer.PutAll(Puts((L2, Payload(Esl(B1)))));

        (await writer.Get(L1)).Should().BeNull();
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L2);
    }

    [Fact]
    public async Task PutAll_KeepsTheAliasesOfAnEntitySentAgainUnchanged()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);

        var response = await writer.PutAll(Puts((L1, Payload(Esl(B1))), (L2, Payload(Esl(B2)))));

        response.NumAddedEntities.Should().Be(1);
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        writer.ResolveAlias(StoreEsl, B2).Should().Be(L2);
    }

    [Fact]
    public async Task PutAllChildren_MayGiveTheAliasOfAChildOfTheSameParent_ToAnother()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        await writer.Put("t/T/s/X/links/L9", Payload(Esl(B2)), null, null);

        await writer.PutAllChildren(Puts((L2, Payload(Esl(B1)))));
        var claimOtherParentsAlias = () => writer.PutAllChildren(Puts((L3, Payload(Esl(B2)))));

        (await writer.Get(L1)).Should().BeNull();
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L2);
        await claimOtherParentsAlias.Should().ThrowAsync<AliasConflictException>("L9 has another parent");
    }

    [Fact]
    public async Task APayloadTheFactoryCantEvaluate_IsRejected_AndKeepsTheStoredAliases()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        var version = writer.GetLatestVersion();

        var put = () => writer.Put(L1, Encoding.UTF8.GetBytes("!garbage"), null, null);
        var putMany = () => writer.PutMany(Puts((L2, Payload(Esl(B2))), (L1, Encoding.UTF8.GetBytes("!garbage"))));

        await put.Should().ThrowAsync<ArgumentException>();
        await putMany.Should().ThrowAsync<ArgumentException>();
        writer.GetLatestVersion().Should().Be(version);
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        writer.ResolveAlias(StoreEsl, B2).Should().BeNull();
    }

    [Fact]
    public async Task CoalescedBatch_FailsOnlyTheIntentWhoseAliasConflicts()
    {
        var writer = await StartWriter(coalescing: true);
        await writer.Put(L1, Payload(Esl(B1)), null, null);

        // Hold the next write in its upload, so the three after it are queued and merged into one batch
        var uploading = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        _storage.BeforeUpload = async () =>
        {
            _storage.BeforeUpload = null;
            uploading.SetResult();
            await release.Task;
        };
        var first = writer.Put("t/T/s/S/links/L0", Payload("x=0"), null, null);
        await uploading.Task;
        var conflicting = writer.Put(L2, Payload(Esl(B1)), null, null);
        var others = new[] { writer.Put(L3, Payload(Esl(B3)), null, null), writer.Put(L4, Payload(Esl(B2)), null, null) };
        release.SetResult();
        await first;

        await ((Func<Task>)(() => conflicting)).Should().ThrowAsync<AliasConflictException>();
        var responses = await Task.WhenAll(others);
        responses.Select(r => r.NewDatasetVersion).Distinct().Should().ContainSingle("the others commit in one delta");
        writer.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        writer.ResolveAlias(StoreEsl, B3).Should().Be(L3);
        writer.ResolveAlias(StoreEsl, B2).Should().Be(L4);
        (await writer.Get(L2)).Should().BeNull();
    }

    [Fact]
    public async Task SharedReaderWriter_ResolvesAnAlias_AsSoonAsTheWriteReturns()
    {
        var writer = await StartWriter(coalescing: true);

        await Parallel.ForEachAsync(Enumerable.Range(0, 200), new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (i, _) =>
            {
                string name = $"t/T/s/S/links/P{i}", alias = $"t/T/s/S/esls/P{i}/link";
                await writer.Put(name, Payload(Esl(alias)), null, null);
                writer.ResolveAlias(StoreEsl, alias).Should().Be(name, $"iteration {i}: the put has returned");

                await writer.Delete(name, null, null);
                writer.ResolveAlias(StoreEsl, alias).Should().BeNull($"iteration {i}: the delete has returned");
            });
    }

    [Fact]
    public async Task Restart_BuildsTheAliasesFromTheIndexAlone()
    {
        var writer = await StartWriter();
        await writer.PutMany(Puts((L1, Payload(Esl(B1))), (L2, Payload(Esl(B2)))));
        await writer.Delete(L2, null, null);
        var version = writer.GetLatestVersion();
        _storage.DataReads = 0;

        var restarted = await StartWriter();

        _storage.DataReads.Should().Be(0);
        restarted.GetLatestVersion().Should().Be(version, "the same definitions need no rebuild");
        restarted.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        restarted.ResolveAlias(StoreEsl, B2).Should().BeNull();
    }

    [Fact]
    public async Task ASnapshot_KeepsTheAliasesAndTheHash()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        await writer.Put(L2, Payload(Esl(B2)), null, null);
        await writer.Delete(L2, null, null);
        await CreateSnapshot();
        _storage.DataReads = 0;

        var restarted = await StartWriter();

        _storage.DataReads.Should().Be(0);
        restarted.GetMainIndexSnapshotVersion().Should().Be(3, "the index starts from the snapshot");
        restarted.GetLatestVersion().Should().Be(3, "the hash in the snapshot matches, so there is no rebuild");
        restarted.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        restarted.ResolveAlias(StoreEsl, B2).Should().BeNull();
        restarted.GetEntityAliases(L1).Should().Equal((StoreEsl, B1));
    }

    [Fact]
    public async Task ChangedDefinitions_AreRebuiltOnce_FromTheData_InOneDelta()
    {
        var writer = await StartWriter([Definition(StoreEsl)]);
        await writer.Put(L1, Payload(Esl(B1), $"{ExternalId}=E1"), null, null);
        await CreateSnapshot();
        await writer.Put(L2, Payload(Esl(B2), $"{ExternalId}=E2"), null, null);
        await writer.Put(L3, Payload("x=3"), null, null);
        var version = writer.GetLatestVersion();
        _storage.DataReads = 0;

        var rebuilt = await StartWriter([Definition(StoreEsl), Definition(ExternalId)], rebuilds: true);

        _storage.DataReads.Should().Be(3, "one pass over the snapshot data, and a read of each of the two newer entities");
        rebuilt.GetLatestVersion().Should().Be(version + 1, "the rebuild is one delta");
        rebuilt.ResolveAlias(ExternalId, "E1").Should().Be(L1);
        rebuilt.ResolveAlias(ExternalId, "E2").Should().Be(L2);
        rebuilt.ResolveAlias(StoreEsl, B1).Should().Be(L1);
        var delta = await ReadDelta(version + 1);
        delta.AliasChanges.Select(c => c.Name).Should().BeEquivalentTo(L1, L2);
        delta.AliasDefinitionsHash.Should().Be(AliasOptions<object>.HashOf([Definition(StoreEsl), Definition(ExternalId)]));

        _storage.DataReads = 0;
        var restartedAgain = await StartWriter([Definition(StoreEsl), Definition(ExternalId)]);

        _storage.DataReads.Should().Be(0);
        restartedAgain.GetLatestVersion().Should().Be(version + 1);
        restartedAgain.ResolveAlias(ExternalId, "E2").Should().Be(L2);
    }

    [Fact]
    public async Task AHostWithoutDefinitions_DoesNotStart_OnADatasetWithAliases()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        var version = writer.GetLatestVersion();

        var withoutDefinitions = await StartWriter([], rebuilds: true, initializes: false, retryInitialization: false);

        await withoutDefinitions.Invoking(w => w.InitializationTask!).Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*registers no alias definitions*");
        (await _provider.ListVersions()).NewestDelta.Should().Be(version, "the aliases aren't dropped");
        (await StartWriter()).ResolveAlias(StoreEsl, B1).Should().Be(L1);
    }

    [Fact]
    public async Task ADatasetWithoutAliases_NeedsNoDefinitions_AndNoRebuild()
    {
        var writer = await StartWriter([]);
        await writer.Put(L1, Payload(Esl(B1)), null, null);
        var version = writer.GetLatestVersion();

        var restarted = await StartWriter([]);

        restarted.GetLatestVersion().Should().Be(version);
        (await _provider.ListVersions()).NewestDelta.Should().Be(version);
        (await ReadDelta(version)).AliasDefinitionsHash.Should().BeEmpty();
    }

    [Fact]
    public async Task ARebuildThatFindsConflicts_GivesTheAliasToTheNewestEntity()
    {
        long conflicts = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == AliasMetrics.MeterName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref conflicts, value));
        listener.Start();

        // Under the old definition the stores have different external ids; under the new one they share one
        var writer = await StartWriter([Definition(ExternalId, "old", "v1")]);
        await writer.Put(L2, Payload("old=A", "new=X", "new=Y"), null, null);
        await writer.Put(L1, Payload("old=B", "new=X"), null, null);

        var rebuilt = await StartWriter([Definition(ExternalId, "new", "v2")], rebuilds: true);

        rebuilt.ResolveAlias(ExternalId, "X").Should().Be(L1, "L1 has the newest version");
        rebuilt.ResolveAlias(ExternalId, "Y").Should().Be(L2, "the loser keeps the rest of its aliases");
        rebuilt.ResolveAlias(ExternalId, "A").Should().BeNull();
        rebuilt.GetEntityAliases(L2).Should().Equal((ExternalId, "Y"));
        Interlocked.Read(ref conflicts).Should().Be(1);

        // Sent again unchanged, the loser keeps the set it owns; evaluating it again would conflict with L1
        await rebuilt.PutMany(Puts((L2, Payload("old=A", "new=X", "new=Y"))));
        await rebuilt.PutAll(Puts((L2, Payload("old=A", "new=X", "new=Y")), (L1, Payload("old=B", "new=X"))));

        rebuilt.ResolveAlias(ExternalId, "X").Should().Be(L1);
        rebuilt.GetEntityAliases(L2).Should().Equal((ExternalId, "Y"));
    }

    [Fact]
    public async Task ARebuild_CountsTheEntitiesItCantEvaluate()
    {
        long unparseable = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == AliasMetrics.MeterName && instrument.Name == "rxdatasets.aliases.rebuild_unparseable")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref unparseable, value));
        listener.Start();

        var writer = await StartWriter([]);
        await writer.Put(L1, Encoding.UTF8.GetBytes("!garbage"), null, null);
        await writer.Put(L2, Payload(Esl(B2)), null, null);

        var rebuilt = await StartWriter([Definition(StoreEsl)], rebuilds: true);

        rebuilt.ResolveAlias(StoreEsl, B2).Should().Be(L2);
        Interlocked.Read(ref unparseable).Should().Be(1);
    }

    [Fact]
    public async Task AReplica_ResolvesExactlyTheWritersAliases_AtEachVersion()
    {
        var writer = await StartWriter();
        var replica = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), _provider, _hub);

        foreach (var write in new Func<Task>[]
                 {
                     () => writer.Put(L1, Payload(Esl(B1)), null, null),
                     () => writer.PutOrDeleteMany(Changes((L1, null), (L2, Payload(Esl(B1), $"{Sic}=1")))),
                     () => writer.Put(L2, Payload(Esl(B2)), null, null),
                 })
        {
            await write();

            replica.GetLatestVersion().Should().Be(writer.GetLatestVersion());
            replica.ListAliases(StoreEsl, "").Should().BeEquivalentTo(writer.ListAliases(StoreEsl, ""));
            replica.ListAliases(Sic, "").Should().BeEquivalentTo(writer.ListAliases(Sic, ""));
            replica.ResolveAlias(StoreEsl, B1).Should().Be(writer.ResolveAlias(StoreEsl, B1));
        }
    }

    [Fact]
    public async Task Reporting_SendsAnAliasChange_ForEachAliasGainedOrLost()
    {
        var writer = await StartWriter();
        await writer.Put(L1, Payload(Esl(B1)), "put", null);
        await writer.Put(L1, Payload(Esl(B2)), "move", null);
        await writer.Delete(L1, "delete", null);

        var reporter = new Mock<IDatasetUpdateReporting<object>>();
        reporter.Setup(r => r.OpenDeltaTransaction(It.IsAny<ulong>())).ReturnsAsync((object?)null);
        reporter.SetupGet(r => r.ReportingIdentifier).Returns("test");
        var plugin = new DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>(_provider,
            Mock.Of<ILogger<DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>>>(), reporter.Object);

        await plugin.HandleNewDeltas(CancellationToken.None);

        reporter.Invocations.Where(i => i.Method.Name == nameof(IDatasetUpdateReporting<object>.OnAliasChanged))
            .Select(i => ((string)i.Arguments[1], (string)i.Arguments[2], (ChangeType)i.Arguments[3], (string?)i.Arguments[4]))
            .Should().Equal(
                (B1, L1, ChangeType.New, "put"),
                (B1, L1, ChangeType.Deleted, "move"),
                (B2, L1, ChangeType.New, "move"),
                (B2, L1, ChangeType.Deleted, "delete"));
    }

    [Fact]
    public async Task Reporting_SendsTheAliasChangesOfARebuild()
    {
        var writer = await StartWriter([Definition(ExternalId, "old", "v1")]);
        await writer.Put(L1, Payload("old=A", "new=X"), "put", null);
        await StartWriter([Definition(ExternalId, "new", "v2")], rebuilds: true);

        var reporter = new Mock<IDatasetUpdateReporting<object>>();
        reporter.Setup(r => r.OpenDeltaTransaction(It.IsAny<ulong>())).ReturnsAsync((object?)null);
        reporter.SetupGet(r => r.ReportingIdentifier).Returns("test");
        var plugin = new DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>(_provider,
            Mock.Of<ILogger<DatasetUpdateReportingPlugin<object, IDatasetUpdateReporting<object>>>>(), reporter.Object);

        await plugin.HandleNewDeltas(CancellationToken.None);

        reporter.Invocations.Where(i => i.Method.Name == nameof(IDatasetUpdateReporting<object>.OnAliasChanged))
            .Select(i => ((string)i.Arguments[1], (string)i.Arguments[2], (ChangeType)i.Arguments[3], (string?)i.Arguments[4]))
            .Should().Equal(
                ("A", L1, ChangeType.New, "put"),
                ("A", L1, ChangeType.Deleted, null),
                ("X", L1, ChangeType.New, null));
    }

    private async Task CreateSnapshot()
    {
        var plugin = new SnapshotCreationPlugin<object>(Mock.Of<ILogger<SnapshotCreationPlugin<object>>>(), _provider,
            Options.Create(new SnapshotCreationConfig { SnapshotInterval = 1 }), new TestScheduler());
        await plugin.HandleNewDeltas(CancellationToken.None);
        (await _provider.ListVersions()).Snapshots.Should().NotBeEmpty();
    }

    /// <summary>Counts the reads of entity data, and can hold an upload.</summary>
    private sealed class CountingStorage(IDiskStorage<object> inner) : IDiskStorage<object>
    {
        private int _dataReads;

        public int DataReads
        {
            get => _dataReads;
            set => _dataReads = value;
        }

        public Func<Task>? BeforeUpload { get; set; }

        private void Count(string objectKey)
        {
            if (objectKey.EndsWith("data.pbs", StringComparison.Ordinal))
                Interlocked.Increment(ref _dataReads);
        }

        public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            Count(objectKey);
            return inner.DownloadBytes(objectKey);
        }

        public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            Count(objectKey);
            return inner.DownloadBytes(objectKey, position, length);
        }

        public Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            Count(objectKey);
            return inner.DownloadWithCallBack(objectKey, callback);
        }

        public async Task UploadBytes(string objectKey, byte[] bytes)
        {
            if (BeforeUpload is { } beforeUpload)
                await beforeUpload();
            await inner.UploadBytes(objectKey, bytes);
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            if (BeforeUpload is { } beforeUpload)
                await beforeUpload();
            await inner.UploadFileStream(objectKey, stream);
        }

        public Task DeleteFile(string objectKey) => inner.DeleteFile(objectKey);
        public Task DeleteFolder(string prefix) => inner.DeleteFolder(prefix);
        public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) => inner.ListObjects(prefix);
        public Task<long> GetObjectContentLength(string objectKey) => inner.GetObjectContentLength(objectKey);
    }
}

internal static class AliasTestExtensions
{
    /// <summary>The aliases an entity owns, from the writer's index.</summary>
    public static IEnumerable<(string Name, string Value)> GetEntityAliases(this DatasetReaderWriter<object> writer,
        string name) =>
        writer.ListAliases(Sic, "").Aliases.Select(a => (Sic, a))
            .Concat(writer.ListAliases(StoreEsl, "").Aliases.Select(a => (StoreEsl, a)))
            .Concat(writer.ListAliases(ExternalId, "").Aliases.Select(a => (ExternalId, a)))
            .Where(alias => writer.ResolveAlias(alias.Item1, alias.Item2) == name);

    private const string StoreEsl = "by_storeesl";
    private const string Sic = "by_sic";
    private const string ExternalId = "by_externalid";
}
