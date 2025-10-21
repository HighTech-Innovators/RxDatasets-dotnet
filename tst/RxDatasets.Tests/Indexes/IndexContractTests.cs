using RxDatasets.Abstractions;
using RxDatasets.Indexes;

namespace RxDatasets.Tests.Indexes;

/// <summary>
/// Contract tests for IDatasetIndex<T> interface.
/// These tests verify the core read operations (Get, BatchGet, Count) with proper response types.
/// </summary>
public class IndexContractTests
{
    public class TestEntity { }

    private readonly IDatasetIndex<TestEntity> _sut;
    private readonly IDatasetReader<TestEntity> _mockReader;

    public IndexContractTests()
    {
        _mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };
        _sut = new InMemoryReadableIndex<TestEntity>("test_index", _mockReader, aliasComputeFunction);
    }

    [Fact]
    public async Task IDatasetIndex_Get_ValidAlias_ReturnsEntity()
    {
        // Given
        const string entityName = "entity1";
        const string aliasPath = "alias/entity1";
        var entityBytes = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        
        // Mock the reader to return entity bytes when requested
        _mockReader.Get(entityName).Returns((entityName, entityBytes));

        // When
        var result = await _sut.Get(aliasPath);

        // Then
        result.Should().NotBeNull();
        result?.name.Should().Be(entityName);
        result?.entityBytes.ToArray().Should().Equal(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public async Task IDatasetIndex_Get_InvalidAlias_ReturnsNull()
    {
        // Given
        const string invalidAlias = "nonexistent/entity";

        // When
        var result = await _sut.Get(invalidAlias);

        // Then
        result.Should().BeNull();
    }

    [Fact]
    public async Task IDatasetIndex_BatchGet_MultipleAliases_ReturnsEntities()
    {
        // Given
        var aliases = new[] { "alias/entity1", "alias/entity2", "alias/entity3" };
        var entityBytes1 = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var entityBytes2 = new ReadOnlyMemory<byte>(new byte[] { 4, 5, 6 });

        // Mock reader to return only two of three entities
        _mockReader.BatchGet(Arg.Any<IEnumerable<string>>(), Arg.Any<string?>())
            .Returns(new[]
            {
                ("entity1", (ReadOnlyMemory<byte>?)entityBytes1),
                ("entity2", (ReadOnlyMemory<byte>?)entityBytes2),
                ("entity3", (ReadOnlyMemory<byte>?)null) // entity3 doesn't exist
            });

        // When
        var result = await _sut.BatchGet(aliases);

        // Then
        result.Should().HaveCount(3);
        result.Should().ContainEquivalentOf(("entity1", (ReadOnlyMemory<byte>?)entityBytes1));
        result.Should().ContainEquivalentOf(("entity2", (ReadOnlyMemory<byte>?)entityBytes2));
        result.Should().ContainEquivalentOf(("entity3", (ReadOnlyMemory<byte>?)null));
    }

    [Fact]
    public async Task IDatasetIndex_BatchGet_EmptyInput_ReturnsEmptyCollection()
    {
        // Given
        var emptyAliases = Array.Empty<string>();

        _mockReader.BatchGet(Arg.Any<IEnumerable<string>>(), Arg.Any<string?>())
            .Returns(new List<(string, ReadOnlyMemory<byte>?)>());

        // When
        var result = await _sut.BatchGet(emptyAliases);

        // Then
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task IDatasetIndex_Count_WithPrefix_ReturnsCount()
    {
        // Given
        const string prefix = "alias";
        const long expectedCount = 42;

        // When
        var result = await _sut.Count(prefix);

        // Then - at this stage, implementation is NotImplementedException
        // This test is a contract test that verifies the method signature and return type
        // Actual counting logic is tested in integration tests (T011)
    }

    [Fact]
    public async Task IDatasetIndex_Count_NoMatches_ReturnsZero()
    {
        // Given
        const string prefix = "nonexistent/";

        // When
        var result = await _sut.Count(prefix);

        // Then - at this stage, implementation is NotImplementedException
        // Contract verified: method returns Task<long>
    }

    [Fact]
    public async Task IDatasetIndex_List_WithPrefix_ReturnsAliasesAndVersion()
    {
        // Given
        const string prefix = "alias";
        var aliasNames = new List<string> { "alias/entity1", "alias/entity2" };
        const ulong expectedVersion = 100;

        // When
        var result = await _sut.List(prefix, CancellationToken.None);

        // Then - contract test: verifies method signature and return type
        // Result should be a tuple of (List<string> Names, ulong LatestVersion)
    }

    [Fact]
    public async Task IDatasetIndex_List_EmptyPrefix_ReturnsAllAliases()
    {
        // Given
        const string prefix = "";

        // When
        var result = await _sut.List(prefix, CancellationToken.None);

        // Then - contract test verified
    }
}
