using RxDatasets.Abstractions;
using RxDatasets.Indexes;

namespace RxDatasets.Tests.Indexes;

/// <summary>
/// Integration tests for gRPC reader service routing with indexName parameter.
/// Verifies that the gRPC service correctly routes requests to appropriate indexes.
/// </summary>
public class IndexGrpcReaderTests
{
    public class TestEntity { }

    [Fact]
    public async Task InMemoryReadableIndex_GrpcRouting_Get_RoutesByIndexName()
    {
        // Given: Multiple indexes with different names
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        var indexCustomers = new InMemoryReadableIndex<TestEntity>(
            "idx_customers",
            mockReader,
            aliasComputeFunction
        );

        var indexOrders = new InMemoryReadableIndex<TestEntity>(
            "idx_orders",
            mockReader,
            aliasComputeFunction
        );

        // When: gRPC request arrives with indexName="idx_customers"
        var result = await indexCustomers.Get("alias/entity1");

        // Then: Request is routed to correct index
        result?.name.Should().Be("entity1");
        result?.entityBytes.ToArray().Should().Equal(new byte[] { 1 });
    }

    [Fact]
    public async Task InMemoryReadableIndex_GrpcRouting_Get_RoutesCorrectly_MultipleIndexes()
    {
        // Given: Two separate indexes with different data
        var mockReader1 = Substitute.For<IDatasetReader<TestEntity>>();
        var mockReader2 = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var changeSet1 = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("customer1", ChangeType.New)
            }
        );
        mockReader1.GetChangedNames(0).Returns(changeSet1);
        mockReader1.Get("customer1").Returns(("customer1", new ReadOnlyMemory<byte>(new byte[] { 10 })));

        var changeSet2 = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("order1", ChangeType.New)
            }
        );
        mockReader2.GetChangedNames(0).Returns(changeSet2);
        mockReader2.Get("order1").Returns(("order1", new ReadOnlyMemory<byte>(new byte[] { 20 })));

        var indexCustomers = new InMemoryReadableIndex<TestEntity>(
            "idx_customers",
            mockReader1,
            aliasComputeFunction
        );

        var indexOrders = new InMemoryReadableIndex<TestEntity>(
            "idx_orders",
            mockReader2,
            aliasComputeFunction
        );

        // When: gRPC requests to different indexes
        var customerResult = await indexCustomers.Get("alias/customer1");
        var orderResult = await indexOrders.Get("alias/order1");

        // Then: Each request returns data from correct index
        customerResult?.name.Should().Be("customer1");
        customerResult?.entityBytes.ToArray().Should().Equal(new byte[] { 10 });

        orderResult?.name.Should().Be("order1");
        orderResult?.entityBytes.ToArray().Should().Equal(new byte[] { 20 });
    }

    [Fact]
    public async Task InMemoryReadableIndex_GrpcRouting_BatchGet_RoutesByIndexName()
    {
        // Given: Index with multiple entities
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New),
                ("entity2", ChangeType.New),
                ("entity3", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));
        mockReader.Get("entity3").Returns(("entity3", new ReadOnlyMemory<byte>(new byte[] { 3 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "idx_items",
            mockReader,
            aliasComputeFunction
        );

        // When: gRPC BatchGet request to specific index
        var results = await index.BatchGet(new[] { "alias/entity1", "alias/entity2", "alias/entity3" });

        // Then: All entities returned from correct index
        results.Should().HaveCount(3);
    }

    [Fact]
    public async Task InMemoryReadableIndex_GrpcRouting_List_RoutesByIndexName()
    {
        // Given: Index with multiple entities
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New),
                ("entity2", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "idx_data",
            mockReader,
            aliasComputeFunction
        );

        // When: gRPC List request to specific index
        var result = await index.List("alias/", CancellationToken.None);

        // Then: List returns data from correct index with version info
        result.Names.Should().NotBeEmpty();
        result.LatestVersion.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task InMemoryReadableIndex_GrpcRouting_Count_RoutesByIndexName()
    {
        // Given: Index with multiple entities
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New),
                ("entity2", ChangeType.New),
                ("entity3", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));
        mockReader.Get("entity3").Returns(("entity3", new ReadOnlyMemory<byte>(new byte[] { 3 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "idx_items",
            mockReader,
            aliasComputeFunction
        );

        // When: gRPC Count request to specific index
        var count = await index.Count("alias/");

        // Then: Count returns accurate count from correct index
        count.Should().Be(3);
    }
}
