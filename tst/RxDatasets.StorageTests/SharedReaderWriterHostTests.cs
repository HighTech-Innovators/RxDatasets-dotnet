using FluentAssertions;
using Google.Protobuf;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests;

/// <summary>
/// Read-after-write through the real <see cref="DtoServerBuilder{TEntity}"/> wiring, with the default (async)
/// NotificationsHub. With a separate DatasetReader the reader catches up on a write in the background, so a read
/// right after the write can miss it; WithSharedReaderWriter serves reads from the index the write just swapped in.
/// </summary>
public class SharedReaderWriterHostTests : IAsyncLifetime
{
    private const int Iterations = 200;
    private IHost _host = null!;
    private IDatasetReader<Demodto> _reader = null!;
    private IDatasetWriter<Demodto> _writer = null!;

    public async Task InitializeAsync()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IDiskStorage<Demodto>>(
            new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>()));
        builder.Services.AddDtoServer<Demodto>().WithSharedReaderWriter().Build();
        builder.Services.AddSingleton<ReadingReporter>();
        builder.Services.AddSingleton<DatasetUpdateReportingPlugin<Demodto, ReadingReporter>>();
        builder.Services.AddHostedService<DatasetUpdateConsumingService<Demodto, DatasetUpdateReportingPlugin<Demodto, ReadingReporter>>>();
        _host = builder.Build();

        // Resolve before starting: they pick up the storage readiness notification sent during StartAsync
        _reader = _host.Services.GetRequiredService<IDatasetReader<Demodto>>();
        _writer = _host.Services.GetRequiredService<IDatasetWriter<Demodto>>();
        _reader.Should().BeSameAs(_writer);

        await _host.StartAsync();
        await WaitUntilInitialized();
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task Put_IsVisibleToGetListAndCountChildren_AsSoonAsPutReturns()
    {
        for (var i = 0; i < Iterations; i++)
        {
            var name = $"parent/child{i:D4}";
            var response = await _writer.Put(name, Entity(name, $"v{i}"));

            (await _reader.Get(name)).Should().NotBeNull($"iteration {i}: the put has returned");
            (await _reader.CountChildren("parent")).Should().Be((uint)(i + 1), $"iteration {i}");
            (await _reader.List("parent", CancellationToken.None)).LatestVersion
                .Should().BeGreaterThanOrEqualTo(response.NewDatasetVersion, $"iteration {i}");
        }
    }

    [Fact]
    public async Task ConcurrentPuts_AreEachVisibleToTheirWriter_AsSoonAsPutReturns()
    {
        // Concurrent puts go through write coalescing (the batched ApplyDelta path)
        await Parallel.ForEachAsync(Enumerable.Range(0, Iterations), new ParallelOptions { MaxDegreeOfParallelism = 8 },
            async (i, _) =>
            {
                var name = $"concurrent/child{i:D4}";
                await _writer.Put(name, Entity(name, $"v{i}"));

                var read = await _reader.Get(name);
                read.Should().NotBeNull($"iteration {i}: the put has returned");
                Demodto.Parser.ParseFrom(read!.Value.entityBytes.Span).SomeString.Should().Be($"v{i}");
            });
    }

    [Fact]
    public async Task Overwrite_IsVisible_AsSoonAsPutReturns()
    {
        const string name = "overwrite/entity";
        for (var i = 0; i < Iterations; i++)
        {
            await _writer.Put(name, Entity(name, $"v{i}"));

            var read = await _reader.Get(name);
            Demodto.Parser.ParseFrom(read!.Value.entityBytes.Span).SomeString.Should().Be($"v{i}", $"iteration {i}");
        }
    }

    [Fact]
    public async Task WaitChangedNames_StartedBeforeAPut_AlwaysWakesForThatPut()
    {
        // This is how AliasDatasetReader follows the dataset: a waiter that misses a write sleeps until the next one
        for (var i = 0; i < Iterations; i++)
        {
            var name = $"waited/child{i:D4}";
            var fromVersion = (await _reader.GetChangedNames(0)).ToVersion;
            var waiter = _reader.WaitChangedNames(fromVersion, CancellationToken.None);

            await _writer.Put(name, Entity(name, $"v{i}"));

            var changes = await waiter.WaitAsync(TimeSpan.FromSeconds(5));
            changes.Changes.Select(c => c.Name).Should().Contain(name, $"iteration {i}");
        }
    }

    [Fact]
    public async Task Reporter_ReadsEachWrite_WhenItIsReported()
    {
        // A reporter (e.g. a change queue) whose consumer reads the current state must not read the state before
        // the write: for a delete it would re-create what was deleted, and no later report repairs that
        var reporter = _host.Services.GetRequiredService<ReadingReporter>();
        for (var i = 0; i < Iterations; i++)
        {
            var name = $"reported/child{i:D4}";
            // Each write waits for its report, so a report never reads a later write
            await _writer.Put(name, Entity(name, $"v{i}"));
            await WaitForReports(reporter, 2 * i + 1);
            await _writer.Delete(name);
            await WaitForReports(reporter, 2 * i + 2);
        }

        reporter.Reports.Where(r => !r.Readable).Should().BeEmpty();
    }

    private static async Task WaitForReports(ReadingReporter reporter, int count)
    {
        for (var attempt = 0; attempt < 500 && reporter.Reports.Count < count; attempt++)
            await Task.Delay(10);
        reporter.Reports.Should().HaveCount(count);
    }

    private static byte[] Entity(string name, string value) => new Demodto { Name = name, SomeString = value }.ToByteArray();

    private sealed class ReadingReporter(IServiceProvider services) : IDatasetUpdateReporting<Demodto>
    {
        public System.Collections.Concurrent.ConcurrentQueue<(string Name, bool Deleted, bool Readable)> Reports { get; } = new();
        public string ReportingIdentifier => "reading-reporter";

        public Task OnNewDto(string name, byte[] bytes, string? trackingId, DateTimeOffset? sla, object? transaction) =>
            Record(name, deleted: false);

        public Task OnChangedDto(string name, byte[] bytes, string? trackingId, DateTimeOffset? sla, object? transaction) =>
            Record(name, deleted: false);

        public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? sla, object? transaction) =>
            Record(name, deleted: true);

        private async Task Record(string name, bool deleted)
        {
            var read = await services.GetRequiredService<IDatasetReader<Demodto>>().Get(name);
            // Puts and deletes of one name alternate, so a read showing the opposite state is the previous write
            Reports.Enqueue((name, deleted, Readable: (read == null) == deleted));
        }
    }

    private async Task WaitUntilInitialized()
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                await _reader.GetChangedNames(0);
                return;
            }
            catch (InvalidOperationException)
            {
                await Task.Delay(50);
            }
        }

        throw new TimeoutException("Dataset did not initialize");
    }
}
