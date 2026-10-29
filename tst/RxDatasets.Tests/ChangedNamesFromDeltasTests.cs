using System.Text;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

/// <summary>
/// The changed names come from the delta indexes, not from version lists in the in-memory index; from version 0 they
/// are the live entities, which the index knows without a delta.
/// </summary>
public class ChangedNamesFromDeltasTests : IDisposable
{
    private const string Changed = "t/T/items/I1/values";
    private const string Kept = "t/T/items/I2/values";
    private const string Deleted = "t/T/items/I3/values";
    private const string Recreated = "t/T/items/I4/values";

    private readonly NotificationsHubSync<object> _hub = new();
    private readonly InMemoryStorageProvider<object> _storage;

    public ChangedNamesFromDeltasTests()
    {
        _storage = new InMemoryStorageProvider<object>(NullLogger<InMemoryStorageProvider<object>>.Instance, _hub);
    }

    public void Dispose() => _hub.Dispose();

    /// <summary>
    /// 1 <see cref="Changed"/> new; 2 <see cref="Kept"/> new; 3 <see cref="Deleted"/> new; 4 <see cref="Changed"/>
    /// changed; 5 <see cref="Deleted"/> deleted; 6 <see cref="Recreated"/> new; 7 deleted; 8 new again.
    /// </summary>
    private async Task<DatasetReaderWriter<object>> WriteChanges()
    {
        var readerWriter = new DatasetReaderWriter<object>(
            _hub, NullLogger<DatasetWriter<object>>.Instance, _storage, Options.Create(new WriterConfig()));
        await _hub.Notify(new Notifications.StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await readerWriter.Put(Changed, Encoding.UTF8.GetBytes("one"), null, null);
        await readerWriter.Put(Kept, Encoding.UTF8.GetBytes("two"), null, null);
        await readerWriter.Put(Deleted, Encoding.UTF8.GetBytes("three"), null, null);
        await readerWriter.Put(Changed, Encoding.UTF8.GetBytes("uno"), null, null);
        await readerWriter.Delete(Deleted, null, null);
        await readerWriter.Put(Recreated, Encoding.UTF8.GetBytes("four"), null, null);
        await readerWriter.Delete(Recreated, null, null);
        await readerWriter.Put(Recreated, Encoding.UTF8.GetBytes("vier"), null, null);
        return readerWriter;
    }

    public static TheoryData<ulong, (string, ChangeType)[]> Expected => new()
    {
        { 0, [(Changed, ChangeType.New), (Kept, ChangeType.New), (Recreated, ChangeType.New)] },
        { 2, [(Changed, ChangeType.Changed), (Recreated, ChangeType.New)] },
        { 3, [(Changed, ChangeType.Changed), (Deleted, ChangeType.Deleted), (Recreated, ChangeType.New)] },
        { 6, [(Recreated, ChangeType.Changed)] },
        { 7, [(Recreated, ChangeType.New)] },
        { 8, [] }
    };

    [Theory]
    [MemberData(nameof(Expected))]
    public async Task ReaderWriter_GetChangedNames_ReturnsTheNetChangeOfEachName(ulong from, (string, ChangeType)[] expected)
    {
        var readerWriter = await WriteChanges();

        var changes = await readerWriter.GetChangedNames(from);

        changes.ToVersion.Should().Be(8);
        changes.Changes.Should().BeEquivalentTo(expected);
    }

    [Theory]
    [MemberData(nameof(Expected))]
    public async Task Reader_GetChangedNames_ReturnsTheNetChangeOfEachName(ulong from, (string, ChangeType)[] expected)
    {
        await WriteChanges();
        var reader = new DatasetReader<object>(NullLogger<DatasetReader<object>>.Instance, _storage, null);
        await reader.StartAsync(CancellationToken.None);

        var changes = await reader.GetChangedNames(from);

        changes.Changes.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task GetChangedNames_FromZero_NeedsNoDeltas()
    {
        var readerWriter = await WriteChanges();
        await _storage.DeleteOlderThan(8);

        var changes = await readerWriter.GetChangedNames(0);

        changes.Changes.Should().BeEquivalentTo(new[] { (Changed, ChangeType.New), (Kept, ChangeType.New), (Recreated, ChangeType.New) });
    }

    [Fact]
    public async Task GetChangedNames_RequiresAResync_WhenADeltaSinceTheVersionIsNoLongerKept()
    {
        var readerWriter = await WriteChanges();
        await _storage.DeleteOlderThan(4);

        var sinceCleanup = await readerWriter.GetChangedNames(4);
        var beforeCleanup = () => readerWriter.GetChangedNames(3);

        sinceCleanup.Changes.Should().BeEquivalentTo(new[] { (Deleted, ChangeType.Deleted), (Recreated, ChangeType.New) });
        (await beforeCleanup.Should().ThrowExactlyAsync<ResyncRequiredException>()).Which.FromVersion.Should().Be(3);
    }

    [Fact]
    public async Task WaitChangedNames_AnswersOnceAWriteLands()
    {
        var readerWriter = await WriteChanges();

        var waiting = readerWriter.WaitChangedNames(8, CancellationToken.None);
        await Task.Delay(50);
        waiting.IsCompleted.Should().BeFalse("nothing changed since version 8");
        await readerWriter.Delete(Kept, null, null);

        (await waiting.WaitAsync(TimeSpan.FromSeconds(5))).Changes.Should().Equal((Kept, ChangeType.Deleted));
    }

    [Fact]
    public async Task ListChanges_AnswersOutOfRange_WhenAResyncIsRequired()
    {
        var dataset = Substitute.For<IDatasetReader<Demodto>>();
        dataset.GetChangedNames(3).Returns<ChangeSet>(_ => throw new ResyncRequiredException(3));
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        configurationSection.Value.Returns("60");
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        var sut = new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(), configuration, dataset, new ServiceCollection().BuildServiceProvider());

        var listing = () => sut.ListChanges(new GrpcReaderServiceImpl<Demodto>.ListChangesRequest(3, 10, "", false), default);

        (await listing.Should().ThrowExactlyAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.OutOfRange);
    }
}
