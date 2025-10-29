using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

public class DataReaderTests
{
    private const string SomeName = "one";
    private static readonly Demodto SomePackage = new() { Name = SomeName };
    private static readonly Demodto ChangedPackage = new() { Name = SomeName, SomeString = SomeName };

    private const string OtherName = "two";
    private static readonly Demodto OtherPackage = new() { Name = OtherName };

    private const string LastName = "tre";
    private static readonly Demodto LastPackage = new() { Name = LastName };

    private readonly GrpcReaderServiceImpl<Demodto> _sut;
    private readonly InMemoryDataset<Demodto> _dataset;

    public DataReaderTests()
    {
        _dataset = new InMemoryDataset<Demodto>();
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        var cacheDuration = "1";
        configurationSection.Value.Returns(cacheDuration);
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        _sut = new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(), configuration, _dataset, default!);
    }

    [Fact]
    public async Task AddingOneShouldLeadToListOfOneNewEntity()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(SomeName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task RequestingChangesAfterLastChangeShouldLeadToEmptyResponse()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = putResult.NewDatasetVersion }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEmpty();
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task AddingOneThenDeletingOneShouldLeadToEmptyResultOverBothChanges()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var deleteResult = await _dataset.Delete(SomeName);

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(deleteResult.NewDatasetVersion);
        result.NewEntities.Should().BeEmpty();
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task AddingOneThenChangingOneShouldLeadToListWithOnlyNewOverBothChanges()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var updateResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(updateResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(SomeName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task AddingOneThenChangingOneShouldLeadToListWithChangeOverTheLastDeltaOnly()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var updateResult = await _dataset.Put(ChangedPackage.Name, ChangedPackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = putResult.NewDatasetVersion }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(updateResult.NewDatasetVersion);
        result.NewEntities.Should().BeEmpty();
        result.ChangedEntities.Should().BeEquivalentTo(SomeName);
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task AddingOneThenDeletingOneShouldLeadToListWithDeleteOverTheLastDeltaOnly()
    {
        // Given
        var putResult = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var deleteResult = await _dataset.Delete(SomeName);

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = putResult.NewDatasetVersion }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(deleteResult.NewDatasetVersion);
        result.NewEntities.Should().BeEmpty();
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEquivalentTo(SomeName);
    }

    [Fact]
    public async Task AddingTwoThenAskingForAllChangesWithDefaultPageSizeShouldNotGivePagedResult()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().NotBe(0);
        result.NewEntities.Should().BeEquivalentTo(SomeName, OtherName);
    }

    [Fact]
    public async Task AddingTwoThenAskingForAllChangesWithPageSizeOneShouldGivePagedResult()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);

        // Then
        result.NextPageToken.Should().NotBeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(0);
        result.NewEntities.Should().BeEquivalentTo(SomeName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task WhenAddingTwoAndRequestingSecondPageShouldGiveLastPage()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var putResult = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);

        // When
        var result = await _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(OtherName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task WhenAddingTwoAndRequestingSecondPageTwiceShouldThrowAnError()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);
        _ = await _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);

        // When
        Func<Task> repeatedCall = () => _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);
        (await repeatedCall.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }

    [Fact]
    public async Task WhenAddingTwoAndRequestingSecondPageAfter5SecondThrowAnError()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var putResult = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);
        await Task.Delay(TimeSpan.FromSeconds(1.1));

        // When
        Func<Task> repeatedCall = () => _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);
        (await repeatedCall.Should().ThrowExactlyAsync<RpcException>()).And.StatusCode.Should().Be(StatusCode.FailedPrecondition);
    }


    [Fact]
    public async Task WhenAddingTwoAndRequestingSecondPageWithin5SecondSucceed()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        var putResult = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);
        await Task.Delay(TimeSpan.FromSeconds(.3));

        // When
        var result = await _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);
        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(OtherName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task WhenAddingThreeAndRequestingThirdPageShouldGiveLastPage()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var putResult = await _dataset.Put(LastPackage.Name, LastPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);
        var nextPage = await _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 1 }, default);

        // When
        var result = await _sut.ListChanges(new() { PageToken = nextPage.NextPageToken, PageSize = 1 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(LastName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task CanIncreasePageSizeOnLaterPages()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var putResult = await _dataset.Put(LastPackage.Name, LastPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);

        // When
        var result = await _sut.ListChanges(new() { PageToken = firstPage.NextPageToken, PageSize = 2 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(OtherName, LastName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task FromDatasetVersionIsIgnoredOnLaterPages()
    {
        // Given
        _ = await _dataset.Put(SomePackage.Name, SomePackage.ToByteArray());
        _ = await _dataset.Put(OtherPackage.Name, OtherPackage.ToByteArray());
        var putResult = await _dataset.Put(LastPackage.Name, LastPackage.ToByteArray());
        var firstPage = await _sut.ListChanges(new() { FromDatasetVersion = 0, PageSize = 1 }, default);

        // When
        var result = await _sut.ListChanges(new() { FromDatasetVersion = 1000, PageToken = firstPage.NextPageToken, PageSize = 2 }, default);

        // Then
        result.NextPageToken.Should().BeNullOrEmpty();
        result.ToDatasetVersion.Should().Be(putResult.NewDatasetVersion);
        result.NewEntities.Should().BeEquivalentTo(OtherName, LastName);
        result.ChangedEntities.Should().BeEmpty();
        result.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task PutAllIncreasesVersionByOnly1()
    {
        // Given
        var packages = new[] { SomePackage, OtherPackage }.ToAsyncEnumerable();

        // When
        var putAllResult = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));

        // Then
        putAllResult.NumAddedEntities.Should().Be(2);
        putAllResult.NewDatasetVersion.Should().Be(1);
    }

    [Fact]
    public async Task IdenticalPutAllIsIdempotent()
    {
        // Given
        var packages = new[] { SomePackage, OtherPackage }.ToAsyncEnumerable();

        // When
        _ = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));
        var putAllResult = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));

        // Then
        putAllResult.NumAddedEntities.Should().Be(0);
        putAllResult.NumDeletedEntities.Should().Be(0);
        putAllResult.NumUpdatedEntities.Should().Be(0);
        putAllResult.NewDatasetVersion.Should().Be(1);
    }

    [Fact]
    public async Task SmallerPutAllDeletesTheMissingPackage()
    {
        // Given
        var packages = new[] { SomePackage, OtherPackage, LastPackage }.ToAsyncEnumerable();
        var fewerPackages = new[] { SomePackage, OtherPackage }.ToAsyncEnumerable();

        // When
        _ = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));
        var putAllResult = await _dataset.PutAll(fewerPackages.Select(x => (x.Name, x.ToByteArray())));

        // Then
        putAllResult.NumAddedEntities.Should().Be(0);
        putAllResult.NumDeletedEntities.Should().Be(1);
        putAllResult.NumUpdatedEntities.Should().Be(0);
        putAllResult.NewDatasetVersion.Should().Be(2);
    }

    [Fact]
    public async Task PutAllWithSameButChangeUpdatesTheChangedPackage()
    {
        // Given
        var packages = new[] { SomePackage, OtherPackage, LastPackage }.ToAsyncEnumerable();
        var packagesWithOneChange = new[] { ChangedPackage, OtherPackage, LastPackage }.ToAsyncEnumerable();

        // When
        _ = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));
        var putAllResult = await _dataset.PutAll(packagesWithOneChange.Select(x => (x.Name, x.ToByteArray())));

        // Then
        putAllResult.NumAddedEntities.Should().Be(0);
        putAllResult.NumDeletedEntities.Should().Be(0);
        putAllResult.NumUpdatedEntities.Should().Be(1);
        putAllResult.NewDatasetVersion.Should().Be(2);
    }

    [Fact]
    public async Task PutAllWithDuplicateAddsOnlyOne()
    {
        // Given
        var packages = new[] { SomePackage, ChangedPackage }.ToAsyncEnumerable();

        // When
        var putAllResult = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));

        // Then
        putAllResult.NumAddedEntities.Should().Be(1);
        putAllResult.NumDeletedEntities.Should().Be(0);
        putAllResult.NumUpdatedEntities.Should().Be(0);
        putAllResult.NewDatasetVersion.Should().Be(1);
    }

    [Fact]
    public async Task PutAllWithDuplicateAddsOnlyTheLastOfThem()
    {
        // Given
        var packages = new[] { SomePackage, ChangedPackage }.ToAsyncEnumerable();

        // When
        _ = await _dataset.PutAll(packages.Select(x => (x.Name, x.ToByteArray())));
        var putResult = await _dataset.Put(ChangedPackage.Name, ChangedPackage.ToByteArray());

        // Then
        putResult.NumAddedEntities.Should().Be(0);
        putResult.NumDeletedEntities.Should().Be(0);
        putResult.NumUpdatedEntities.Should().Be(0);
        putResult.NewDatasetVersion.Should().Be(1);
    }
}
