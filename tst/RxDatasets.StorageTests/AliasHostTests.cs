using FluentAssertions;
using Google.Protobuf;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests;

/// <summary>Aliases registered with AddDatasetAliases, checked on write by the writer DtoServerBuilder wires up.</summary>
public class AliasHostTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AConflictingWrite_IsRejected(bool sharedReaderWriter)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IDiskStorage<Demodto>>(
            new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>()));
        builder.Services.AddDatasetAliases(Demodto.Parser, ["things/{thingid}"], ("by_value", "values/{.some_string}"));
        var dtoServer = builder.Services.AddDtoServer<Demodto>();
        if (sharedReaderWriter)
            dtoServer.WithSharedReaderWriter();
        dtoServer.Build();
        using var host = builder.Build();
        var writer = host.Services.GetRequiredService<IDatasetWriter<Demodto>>();
        await host.StartAsync();

        await WaitUntilInitialized(() => writer.Put("things/A", Entity("things/A", "v")));
        var conflicting = () => writer.Put("things/B", Entity("things/B", "v"));

        await conflicting.Should().ThrowAsync<AliasConflictException>();
        await host.StopAsync();
    }

    private static byte[] Entity(string name, string value) => new Demodto { Name = name, SomeString = value }.ToByteArray();

    private static async Task WaitUntilInitialized(Func<Task> write)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            try
            {
                await write();
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
