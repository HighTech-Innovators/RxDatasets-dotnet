using FluentAssertions;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;
using RxDatasets.TestImplementations.InMemoryProvider;
using static RxDatasets.StorageTests.AliasTestHost;

namespace RxDatasets.StorageTests;

/// <summary>
/// Aliases through the shared reader/writer of a host (WithSharedReaderWriter): every write is checked against the
/// aliases, and the alias readers serve it as soon as the write returns.
/// </summary>
public class AliasSharedReaderWriterHostTests : IAsyncLifetime
{
    private const int Iterations = 200;
    protected const string A = "things/A";
    protected const string B = "things/B";
    protected const string C = "things/C";

    private AliasTestHost _host = null!;

    protected virtual bool Coalescing => false;

    private protected GatedStorage Storage { get; } =
        new(new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>()));

    protected IDatasetWriter<Demodto> Writer => _host.Writer;
    protected IDatasetReader<Demodto> Reader => _host.Reader;
    protected IDatasetReader<Demodto> ByValueReader => _host.Alias(ByValue);

    public async Task InitializeAsync() =>
        _host = await AliasTestHost.Start(Storage, new AliasHostOptions { Coalescing = Coalescing });

    public async Task DisposeAsync() => await _host.DisposeAsync();

    protected async Task<string?> Owner(string alias) => (await ByValueReader.Get(alias))?.name;

    // ==================== WRITE, THEN READ BY ALIAS AT ONCE ====================

    [Fact]
    public async Task AnOverwrite_MovesTheAlias_AsSoonAsPutReturns()
    {
        for (var i = 0; i < Iterations; i++)
        {
            await Writer.Put(A, Entity(A, $"v{i}"), null, null);

            (await Owner($"values/v{i}")).Should().Be(A, $"iteration {i}: the put has returned");
            if (i > 0)
                (await Owner($"values/v{i - 1}")).Should().BeNull($"iteration {i}: the old value is gone");
        }
    }

    [Fact]
    public async Task ConcurrentPuts_AreEachResolvableByAlias_AsSoonAsTheirPutReturns()
    {
        await Parallel.ForEachAsync(Enumerable.Range(0, Iterations), new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (i, _) =>
            {
                var name = $"things/P{i}";
                await Writer.Put(name, Entity(name, $"p{i}"), null, null);

                (await Owner($"values/p{i}")).Should().Be(name, $"iteration {i}: the put has returned");
            });
    }

    [Fact]
    public async Task ListAndCountChildren_OfAnAliasParent_FollowTheWrites()
    {
        var put = await Writer.PutMany(Puts((A, "shelf1/a"), (B, "shelf1/b"), (C, "shelf2/c")));

        var (names, version) = await ByValueReader.List("values/shelf1", CancellationToken.None);
        names.Should().Equal("values/shelf1/a", "values/shelf1/b");
        version.Should().Be(put.NewDatasetVersion);
        (await ByValueReader.CountChildren("values/shelf1")).Should().Be(2);

        await Writer.Delete(B, null, null);

        (await ByValueReader.List("values/shelf1", CancellationToken.None)).Names.Should().Equal("values/shelf1/a");
        (await ByValueReader.CountChildren("values/shelf1")).Should().Be(1);
    }

    [Fact]
    public async Task BatchGet_ReturnsTheEntitiesOfTheKnownAliases()
    {
        await Writer.PutMany(Puts((A, "a"), (B, "b")));

        var read = await ByValueReader.BatchGet(["values/a", "values/unknown", "values/b"], null);

        read.Select(r => r.name).Should().BeEquivalentTo(A, B);
        read.Should().AllSatisfy(r => r.entityBytes.Should().NotBeNull());
    }

    [Fact]
    public async Task GetChangedNames_ListsAllAliases_AtTheVersionOfTheWrite()
    {
        var put = await Writer.PutMany(Puts((A, "a"), (B, "b")));

        var changes = await ByValueReader.GetChangedNames(0);

        changes.ToVersion.Should().Be(put.NewDatasetVersion);
        changes.Changes.Should().BeEquivalentTo([("values/a", ChangeType.New), ("values/b", ChangeType.New)]);
    }

    [Fact]
    public async Task WaitChangedNames_StartedBeforeAPut_AlwaysWakesForThatPut()
    {
        for (var i = 0; i < 50; i++)
        {
            var fromVersion = (await ByValueReader.GetChangedNames(0)).ToVersion;
            var waiter = ByValueReader.WaitChangedNames(fromVersion, CancellationToken.None);

            var name = $"things/W{i}";
            var put = await Writer.Put(name, Entity(name, $"w{i}"), null, null);

            var changes = await waiter.WaitAsync(TimeSpan.FromSeconds(5));
            changes.ToVersion.Should().Be(put.NewDatasetVersion, $"iteration {i}");
        }
    }

    [Fact]
    public async Task GetAliasesFor_GivesTheAliasesTheWriteStored()
    {
        var dto = new Demodto { Name = A, SomeString = "x" };
        await Writer.Put(A, Entity(A, "x"), null, null);

        var aliases = _host.Services.GetAliasesFor(A, dto);

        aliases.Should().BeEquivalentTo([(ByValue, "values/x"), (ByThing, "values/A")]);
        foreach (var (aliasName, alias) in aliases)
            (await _host.Alias(aliasName).Get(alias))!.Value.name.Should().Be(A, $"{aliasName} {alias}");
    }

    // ==================== CONFLICTING WRITES ====================

    [Fact]
    public async Task AConflict_NamesTheAliasAndBothEntities_AndIsNotAnInvalidOperation()
    {
        await Writer.Put(A, Entity(A, "x"), null, null);

        var conflicting = () => Writer.Put(B, Entity(B, "x"), null, null);

        var thrown = await conflicting.Should().ThrowAsync<AliasConflictException>();
        thrown.Which.Should().BeEquivalentTo(new { AliasName = ByValue, Alias = "values/x", EntityName = B, OwnerName = A });
        thrown.Which.Should().NotBeAssignableTo<InvalidOperationException>(
            "callers take an InvalidOperationException for a dataset that is not ready, and retry");
    }

    [Fact]
    public async Task AnEntity_MayPutAgain_WithTheAliasItOwns()
    {
        var first = await Writer.Put(A, Entity(A, "x", someInt: 1), null, null);

        var second = await Writer.Put(A, Entity(A, "x", someInt: 2), null, null);

        second.NewDatasetVersion.Should().BeGreaterThan(first.NewDatasetVersion);
        var read = await ByValueReader.Get("values/x");
        read!.Value.name.Should().Be(A);
        Demodto.Parser.ParseFrom(read.Value.entityBytes.Span).SomeInt.Should().Be(2);
    }

    [Fact]
    public async Task ConcurrentClaimsOfOneAlias_LetExactlyOneEntityWin()
    {
        const int rounds = 20;
        const int contenders = 8;
        for (var round = 0; round < rounds; round++)
        {
            var value = $"contested{round}";
            var names = Enumerable.Range(0, contenders).Select(c => $"things/R{round}x{c}").ToList();

            var outcomes = await Task.WhenAll(names.Select(name => Task.Run(async () =>
            {
                try
                {
                    await Writer.Put(name, Entity(name, value), null, null);
                    return (Name: name, Error: (Exception?)null);
                }
                catch (Exception e)
                {
                    return (Name: name, Error: e);
                }
            })));

            var winner = outcomes.Where(o => o.Error == null).Should().ContainSingle($"round {round}").Which.Name;
            outcomes.Where(o => o.Error != null).Should()
                .AllSatisfy(o => o.Error.Should().BeOfType<AliasConflictException>(), $"round {round}");
            (await Owner($"values/{value}")).Should().Be(winner, $"round {round}");
            foreach (var loser in names.Where(name => name != winner))
                (await Reader.Get(loser)).Should().BeNull($"round {round}: {loser} lost");
        }
    }

    [Fact]
    public async Task EntitiesWithoutTheAliasedField_ClaimNothing_AndDontConflict()
    {
        await Writer.Put(A, Entity(A, ""), null, null);
        await Writer.Put(B, Entity(B, ""), null, null);

        (await Reader.CountChildren("things")).Should().Be(2);
        (await ByValueReader.GetChangedNames(0)).Changes.Should().BeEmpty();
    }
}

/// <summary>The same, with write coalescing: concurrent writes are merged into one delta.</summary>
public class AliasSharedReaderWriterCoalescingHostTests : AliasSharedReaderWriterHostTests
{
    protected override bool Coalescing => true;
}

/// <summary>Passes everything to a storage, and can hold an upload.</summary>
internal sealed class GatedStorage(IDiskStorage<Demodto> inner) : IDiskStorage<Demodto>
{
    public Func<string, Task>? BeforeUpload { get; set; }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        if (BeforeUpload is { } beforeUpload)
            await beforeUpload(objectKey);
        await inner.UploadBytes(objectKey, bytes);
    }

    public async Task UploadFileStream(string objectKey, Stream stream)
    {
        if (BeforeUpload is { } beforeUpload)
            await beforeUpload(objectKey);
        await inner.UploadFileStream(objectKey, stream);
    }

    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey) => inner.DownloadBytes(objectKey);

    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length) =>
        inner.DownloadBytes(objectKey, position, length);

    public Task DownloadWithCallBack(string objectKey, Action<Stream> callback) =>
        inner.DownloadWithCallBack(objectKey, callback);

    public Task DeleteFile(string objectKey) => inner.DeleteFile(objectKey);
    public Task DeleteFolder(string prefix) => inner.DeleteFolder(prefix);
    public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) => inner.ListObjects(prefix);
    public Task<long> GetObjectContentLength(string objectKey) => inner.GetObjectContentLength(objectKey);
}
