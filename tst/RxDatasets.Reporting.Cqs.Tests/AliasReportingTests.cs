using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Hightechinnovators.Dtos.DemodtoV1;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Hti.RxDatasets.Cqs.V1;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

/// <summary>
/// Alias changes reach CQS: each gained or lost alias is a notification under its alias name, with the alias
/// value as the id, so a queue subscribed to <c>type:alias</c> hears about it. Deltas come in both index formats,
/// the tree a writer stores today and the legacy flat lists of older ones.
/// </summary>
public class AliasReportingTests
{
    [Fact]
    public async Task AnAliasChangeIsANotificationUnderItsAliasName_GainedOrLost()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object transaction = (await reporter.OpenDeltaTransaction(7))!;
        DateTimeOffset sla = new(2026, 10, 20, 13, 14, 15, TimeSpan.Zero);

        await reporter.OnNewDto("things/A", [1], "track-1", sla, transaction);
        await reporter.OnAliasChanged("by_value", "values/x", "things/A", ChangeType.New, "track-1", sla, transaction);
        await reporter.OnAliasChanged("by_value", "values/y", "things/A", ChangeType.Deleted, null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);

        DtoChangeNotification[] sent = server.Service.Requests.Should().ContainSingle().Which.Notifications.ToArray();
        sent.Select(n => (n.AliasName, n.DtoId)).Should()
            .Equal(("", "things/A"), ("by_value", "values/x"), ("by_value", "values/y"));
        sent[1].DtoType.Should().Be("demodto.v1");
        sent[1].UrnCase.Should().Be(DtoChangeNotification.UrnOneofCase.DtoId);
        sent[1].TrackingId.Should().Be("track-1");
        sent[1].SlaTimestamp.Should().Be(Timestamp.FromDateTimeOffset(sla));
        sent[2].TrackingId.Should().Be("");
        sent[2].SlaTimestamp.Should().BeNull();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheBatchingPluginSendsTheEntitiesAndTheirAliasChangesToCqs(bool tree)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await AliasDeltas(tree);
        CqsReporter<Demodto> reporter = CreateReporter(server);

        await new DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>>(storage,
            NullLogger<DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>>>.Instance, reporter)
            .HandleNewDeltas(CancellationToken.None);

        // Four deltas share one transaction, hence one call. The alias changes follow their entity.
        PushBatchRequest request = server.Service.Requests.Should().ContainSingle().Which;
        request.Notifications.Select(n => (n.AliasName, n.DtoId)).Should().Equal(
            ("", "entity/1"), ("by_value", "values/a"),
            ("", "entity/2"), ("by_value", "values/b"), ("by_value", "values/c"),
            ("", "entity/3"), ("by_value", "values/d"),
            ("by_value", "values/f"), ("by_value", "values/e"));
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(4ul);
    }

    /// <summary>
    /// Four deltas with aliases, as a writer stores them: a new entity gaining one, a changed entity trading one
    /// for another, a deleted entity releasing one, and an alias-only change from a rebuild.
    /// </summary>
    internal static async Task<InMemoryStorageProvider<Demodto>> AliasDeltas(bool tree)
    {
        InMemoryStorageProvider<Demodto> storage = new(
            NullLogger<InMemoryStorageProvider<Demodto>>.Instance, new NotificationsHub<Demodto>());
        byte[] data = new Demodto { SomeInt = 1 }.ToByteArray();

        async Task Store(ulong version, DeltaIndex index, byte[] bytes)
        {
            index.DeltaVersion = version;
            if (tree)
            {
                index.ConvertToTree();
            }

            await storage.StoreDelta(version, index.ToByteArray(), bytes);
        }

        DeltaIndex first = new();
        DeltaEntity created = new() { Name = "entity/1", Position = 0, Length = data.Length };
        created.AddedAliases.Add(Alias("values/a"));
        first.NewEntities.Add(created);
        await Store(1, first, data);

        DeltaIndex second = new();
        DeltaEntity changed = new() { Name = "entity/2", Position = 0, Length = data.Length, TrackingId = "track-2" };
        changed.RemovedAliases.Add(Alias("values/b"));
        changed.AddedAliases.Add(Alias("values/c"));
        second.ChangedEntities.Add(changed);
        await Store(2, second, data);

        DeltaIndex third = new();
        DeletedEntity deleted = new() { Name = "entity/3" };
        deleted.RemovedAliases.Add(Alias("values/d"));
        third.DeletedEntities.Add(deleted);
        await Store(3, third, []);

        DeltaIndex fourth = new();
        AliasOnlyChange rebuilt = new() { Name = "entity/4" };
        rebuilt.AddedAliases.Add(Alias("values/e"));
        rebuilt.RemovedAliases.Add(Alias("values/f"));
        fourth.AliasChanges.Add(rebuilt);
        await Store(4, fourth, []);

        return storage;
    }

    private static AliasRef Alias(string value) => new() { Name = "by_value", Value = value };

    private static CqsReporter<Demodto> CreateReporter(FakeCqsServer server) =>
        new(server.Client, Options.Create(new CqsReportingConfig
        {
            // The deltas are stored before the plugin first runs: replay them
            Endpoint = server.Endpoint, DurabilityTimeout = TimeSpan.FromSeconds(5), ReplayRetainedHistory = true
        }), NullLogger<CqsReporter<Demodto>>.Instance);
}
