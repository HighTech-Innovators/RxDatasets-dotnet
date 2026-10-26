using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// A <c>ListChanges</c> that waits for changes and is cancelled before any arrive (VLOW-40).
/// </summary>
public class DataReaderCancelledListChangesTests
{
    private readonly IHostApplicationLifetime _lifetime = Substitute.For<IHostApplicationLifetime>();
    private readonly GrpcReaderServiceImpl<Demodto> _sut;

    public DataReaderCancelledListChangesTests()
    {
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        configurationSection.Value.Returns("1");
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        _sut = new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            _lifetime, configuration, new InMemoryDataset<Demodto>(), default!);
    }

    [Fact]
    public async Task CancelledByTheClientAnswersWithEmptyListsAndNoToken()
    {
        // Given
        using var client = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0, WaitForChanges = true }, client.Token);

        // Then
        result.NewEntities.Should().NotBeNull().And.BeEmpty();
        result.ChangedEntities.Should().NotBeNull().And.BeEmpty();
        result.DeletedEntities.Should().NotBeNull().And.BeEmpty();
        result.NextPageToken.Should().BeEmpty();
    }

    [Fact]
    public async Task CancelledByAStoppingServerAnswersUnavailable()
    {
        // Given
        using var stopping = new CancellationTokenSource();
        _lifetime.ApplicationStopping.Returns(stopping.Token);
        stopping.CancelAfter(TimeSpan.FromMilliseconds(200));

        // When
        Func<Task> waiting = () => _sut.ListChanges(new() { FromDatasetVersion = 0, WaitForChanges = true }, CancellationToken.None);

        // Then
        (await waiting.Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(StatusCode.Unavailable);
    }
}
