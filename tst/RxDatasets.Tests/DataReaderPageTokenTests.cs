using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// A page token continues only the listing it was issued for (VLOW-36).
/// </summary>
public class DataReaderPageTokenTests
{
    private const string ParentA = "a";
    private const string ParentB = "b";
    private const string Alias = "other";

    private readonly InMemoryDataset<Demodto> _dataset = new();
    private readonly InMemoryDataset<Demodto> _aliasedDataset = new();
    private readonly GrpcReaderServiceImpl<Demodto> _sut;

    public DataReaderPageTokenTests()
    {
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        configurationSection.Value.Returns("60");
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        var services = new ServiceCollection()
            .AddKeyedSingleton<IDatasetReader<Demodto>>(Alias, _aliasedDataset)
            .BuildServiceProvider();
        _sut = new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(), configuration, _dataset, services);
    }

    private static async Task PutUnder(InMemoryDataset<Demodto> dataset, string parent, params string[] names)
    {
        foreach (var name in names)
        {
            var id = parent + "/" + name;
            _ = await dataset.Put(id, new Demodto { Name = id }.ToByteArray(), null, null);
        }
    }

    [Fact]
    public async Task ContinuingAListingWithAnotherParentShouldBeRefused()
    {
        // Given
        await PutUnder(_dataset, ParentA, "1", "2");
        await PutUnder(_dataset, ParentB, "1", "2");
        var (_, firstPageToken) = await _sut.ListChildren(ParentA, 1, "", default);

        // When
        Func<Task> continuation = () => _sut.ListChildren(ParentB, 1, firstPageToken!, default);

        // Then
        (await continuation.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task ContinuingAListingWithAnotherAliasShouldBeRefused()
    {
        // Given
        await PutUnder(_dataset, ParentA, "1", "2");
        await PutUnder(_aliasedDataset, ParentA, "1", "2");
        var (_, firstPageToken) = await _sut.ListChildren(ParentA, 1, "", default);

        // When
        Func<Task> continuation = () => _sut.ListChildren(ParentA, 1, firstPageToken!, Alias, default);

        // Then
        (await continuation.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task GettingChildrenWithAnotherParentShouldBeRefused()
    {
        // Given
        await PutUnder(_dataset, ParentA, "1", "2");
        await PutUnder(_dataset, ParentB, "1", "2");
        var (_, firstPageToken) = await _sut.GetChildren(ParentA, 1, "", default);

        // When
        Func<Task> continuation = () => _sut.GetChildren(ParentB, 1, firstPageToken!, default);

        // Then
        (await continuation.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task ARefusedContinuationShouldUseUpTheToken()
    {
        // Given
        await PutUnder(_dataset, ParentA, "1", "2");
        await PutUnder(_dataset, ParentB, "1", "2");
        var (_, firstPageToken) = await _sut.ListChildren(ParentA, 1, "", default);
        Func<Task> foreign = () => _sut.ListChildren(ParentB, 1, firstPageToken!, default);
        await foreign.Should().ThrowExactlyAsync<RpcException>();

        // When
        Func<Task> own = () => _sut.ListChildren(ParentA, 1, firstPageToken!, default);

        // Then
        (await own.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task ContinuingAListingWithTheSameParentAndAnotherPageSizeShouldSucceed()
    {
        // Given
        await PutUnder(_dataset, ParentA, "1", "2", "3");
        await PutUnder(_dataset, ParentB, "1");
        var (firstPage, firstPageToken) = await _sut.ListChildren(ParentA, 1, "", default);

        // When
        var (rest, nextPageToken) = await _sut.ListChildren(ParentA, 5, firstPageToken!, default);

        // Then
        nextPageToken.Should().BeNullOrEmpty();
        firstPage.Concat(rest).Should().BeEquivalentTo("a/1", "a/2", "a/3");
    }
}
