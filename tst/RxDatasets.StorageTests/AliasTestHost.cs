using Google.Protobuf;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;

namespace RxDatasets.StorageTests;

/// <summary>How <see cref="AliasTestHost.Start"/> wires the host.</summary>
internal sealed record AliasHostOptions
{
    /// <summary>The aliases registered with AddDatasetAliases; null registers none.</summary>
    public (string AliasName, string AliasPattern)[]? Aliases { get; init; } = AliasTestHost.DefaultAliases;

    public bool Shared { get; init; } = true;
    public bool Coalescing { get; init; }
    public bool Snapshots { get; init; }
    public bool Cleanup { get; init; }
    public Dictionary<string, string?> Configuration { get; init; } = [];

    /// <summary>Runs after the DtoServerBuilder, so it can replace what that registered.</summary>
    public Action<IServiceCollection>? ConfigureServices { get; init; }

    public ILoggerProvider? Logger { get; init; }
    public bool WaitUntilInitialized { get; init; } = true;
}

/// <summary>
/// A host wired like a real server: <see cref="DtoServerBuilder{TEntity}"/> with the aliases of AddDatasetAliases, on a
/// storage that outlives it, so a test can restart on the same data. Entities are named things/{thingid} or
/// groups/{groupid}/things/{thingid}.
/// </summary>
internal sealed class AliasTestHost : IAsyncDisposable
{
    /// <summary>The entity's some_string; empty gives no alias.</summary>
    public const string ByValue = "by_value";

    /// <summary>The entity's thing id, so never in conflict; in the same value space as <see cref="ByValue"/>.</summary>
    public const string ByThing = "by_thing";

    public static readonly string[] IdPatterns = ["things/{thingid}", "groups/{groupid}/things/{thingid}"];

    public static readonly (string AliasName, string AliasPattern)[] DefaultAliases =
        [(ByValue, "values/{.some_string}"), (ByThing, "values/{thingid}")];

    private readonly IHost _host;

    private AliasTestHost(IHost host)
    {
        _host = host;
        Writer = host.Services.GetRequiredService<IDatasetWriter<Demodto>>();
        Reader = host.Services.GetRequiredService<IDatasetReader<Demodto>>();
    }

    public IServiceProvider Services => _host.Services;
    public IDatasetWriter<Demodto> Writer { get; }
    public IDatasetReader<Demodto> Reader { get; }

    public IDatasetReader<Demodto> Alias(string aliasName) => Services.GetDatasetReader<Demodto>(aliasName)!;

    public static async Task<AliasTestHost> Start(IDiskStorage<Demodto> storage, AliasHostOptions? options = null)
    {
        options ??= new AliasHostOptions();
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        if (options.Logger != null)
            builder.Logging.AddProvider(options.Logger);
        builder.Configuration.AddInMemoryCollection(options.Configuration);
        builder.Services.AddSingleton(storage);
        if (options.Aliases != null)
            builder.Services.AddDatasetAliases(Demodto.Parser, IdPatterns, options.Aliases);

        var dtoServer = builder.Services.AddDtoServer<Demodto>();
        if (options.Shared)
            dtoServer.WithSharedReaderWriter();
        if (options.Coalescing)
            dtoServer.WithWriteCoalescing();
        if (options.Snapshots)
            dtoServer.WithSnapshot();
        if (options.Cleanup)
            dtoServer.WithCleanup();
        dtoServer.Build();
        options.ConfigureServices?.Invoke(builder.Services);

        // Resolved before starting: they pick up the storage readiness notification sent during StartAsync
        var host = new AliasTestHost(builder.Build());
        foreach (var aliasName in options.Aliases?.Select(a => a.AliasName).Distinct() ?? [])
            host.Alias(aliasName);

        await host._host.StartAsync();
        if (options.WaitUntilInitialized)
            await Eventually(host.IsInitialized, "the dataset initializes");
        return host;
    }

    public async ValueTask DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    public async Task<ulong> NewestDelta() =>
        (await Services.GetRequiredService<IStorageProvider<Demodto>>().ListVersions()).NewestDelta;

    public async Task<DeltaIndex> ReadDelta(ulong version) =>
        DeltaIndex.Parser.ParseFrom(
            (await Services.GetRequiredService<IStorageProvider<Demodto>>().ReadDeltaIndex(version)).Span);

    /// <summary>Waits for a snapshot at <paramref name="version"/>.</summary>
    public Task WaitForSnapshot(ulong version) =>
        Eventually(async () => (await Services.GetRequiredService<IStorageProvider<Demodto>>().ListVersions()).Snapshots
            .Any(s => s.Version == version), $"a snapshot at version {version}");

    private async Task<bool> IsInitialized()
    {
        if (Writer is not DatasetWriter<Demodto> { InitializationTask.IsCompletedSuccessfully: true })
            return false;

        try
        {
            await Reader.GetChangedNames(0);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    public static async Task Eventually(Func<Task<bool>> condition, string what, int timeoutSeconds = 10)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting until {what}");
            await Task.Delay(20);
        }
    }

    public static byte[] Entity(string name, string value, int someInt = 0) =>
        new Demodto { Name = name, SomeString = value, SomeInt = someInt }.ToByteArray();

    /// <summary>Puts with <see cref="Entity"/> bytes: (name, some_string).</summary>
    public static IAsyncEnumerable<(string, byte[], string?, DateTimeOffset?)> Puts(
        params (string Name, string Value)[] entities) =>
        entities.Select(e => (e.Name, Entity(e.Name, e.Value), (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable();

    /// <summary>Puts with <see cref="Entity"/> bytes, and deletes for a null value.</summary>
    public static IAsyncEnumerable<(string, byte[]?, string?, DateTimeOffset?)> Changes(
        params (string Name, string? Value)[] changes) =>
        changes.Select(c => (c.Name, c.Value == null ? null : Entity(c.Name, c.Value), (string?)null, (DateTimeOffset?)null))
            .ToAsyncEnumerable();
}
