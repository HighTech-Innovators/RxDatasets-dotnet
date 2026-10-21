using RxDatasets.AliasingTests.Types;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.AliasingTests;

/// <summary>
/// Unit tests for AliasDatasetReader DI registration and retrieval.
/// 
/// Tests verify that:
/// - Multiple alias readers can be registered with different alias names
/// - GetDatasetReader retrieves the correct reader by alias name
/// - Unregistered alias names return null
/// - Both aliased and non-aliased access work together
/// </summary>
public class AliasDatasetReaderDITests
{
    /// <summary>
    /// Extract aliases from a TestEntity protobuf message
    /// </summary>
    private static IEnumerable<string> ExtractAliases(ReadOnlyMemory<byte> bytes)
    {
        var entity = TestEntity.Parser.ParseFrom(bytes.Span);
        return entity.Aliases;
    }

    [Fact]
    public async Task Should_Register_And_Retrieve_Single_Alias_Reader()
    {
        // Arrange - set up DI container
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        // Create and register dataset
        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        // Register alias reader with name "prod-aliases"
        services.AddDatasetAlias<TestEntity>("prod-aliases", ExtractAliases);

        var serviceProvider = services.BuildServiceProvider();

        // Create test data
        var relation = new TestEntity { Id = "relation/1" };
        relation.Aliases.Add("prod-product-1");
        await dataset.Put(relation.Id, relation.ToByteArray(), null, null);

        // Act - retrieve the registered alias reader
        var aliasReader = serviceProvider.GetDatasetReader<TestEntity>("prod-aliases");

        // Assert
        aliasReader.Should().NotBeNull("alias reader should be registered with name 'prod-aliases'");
        aliasReader.Should().BeOfType<AliasDatasetReader<TestEntity>>();

        // Verify the alias reader works
        var result = await aliasReader!.Get("prod-product-1");
        result.Should().NotBeNull("alias should resolve");
        var entity = TestEntity.Parser.ParseFrom(result!.Value.entityBytes.Span);
        entity.Id.Should().Be("relation/1");
    }

    [Fact]
    public async Task Should_Register_Multiple_Alias_Readers_With_Different_Names()
    {
        // Arrange - set up DI container
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        // Register two different alias readers
        services.AddDatasetAlias<TestEntity>("staging-aliases", ExtractAliases);
        services.AddDatasetAlias<TestEntity>("production-aliases", ExtractAliases);

        var serviceProvider = services.BuildServiceProvider();

        // Create test data
        var relation = new TestEntity { Id = "relation/2" };
        relation.Aliases.Add("shared-product");
        await dataset.Put(relation.Id, relation.ToByteArray(), null, null);

        // Act - retrieve both alias readers
        var stagingReader = serviceProvider.GetDatasetReader<TestEntity>("staging-aliases");
        var prodReader = serviceProvider.GetDatasetReader<TestEntity>("production-aliases");

        // Assert
        stagingReader.Should().NotBeNull("staging alias reader should be registered");
        prodReader.Should().NotBeNull("production alias reader should be registered");
        stagingReader.Should().NotBeSameAs(prodReader, "each alias reader should be a separate instance");

        // Both should work independently
        var stagingResult = await stagingReader!.Get("shared-product");
        var prodResult = await prodReader!.Get("shared-product");

        stagingResult.Should().NotBeNull();
        prodResult.Should().NotBeNull();

        var stagingEntity = TestEntity.Parser.ParseFrom(stagingResult!.Value.entityBytes.Span);
        var prodEntity = TestEntity.Parser.ParseFrom(prodResult!.Value.entityBytes.Span);

        stagingEntity.Id.Should().Be("relation/2");
        prodEntity.Id.Should().Be("relation/2");
    }

    [Fact]
    public async Task Should_Return_Null_For_Unregistered_Alias_Name()
    {
        // Arrange - set up DI container
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        // Register one alias reader
        services.AddDatasetAlias<TestEntity>("registered-name", ExtractAliases);

        var serviceProvider = services.BuildServiceProvider();

        // Act - try to retrieve an unregistered alias reader
        var unregisteredReader = serviceProvider.GetDatasetReader<TestEntity>("unregistered-name");

        // Assert
        unregisteredReader.Should().BeNull("unregistered alias reader should return null");
    }

    [Fact]
    public async Task Should_Support_Direct_Dataset_Access_Alongside_Alias_Readers()
    {
        // Arrange - set up DI container with both direct dataset and alias readers
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset); // Direct non-keyed access
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        // Register alias reader
        services.AddDatasetAlias<TestEntity>("my-aliases", ExtractAliases);

        var serviceProvider = services.BuildServiceProvider();

        // Create test data
        var relation = new TestEntity { Id = "relation/3" };
        relation.Aliases.Add("alias-name");
        await dataset.Put(relation.Id, relation.ToByteArray(), null, null);

        // Act
        // Access the dataset directly (non-keyed)
        var directReader = serviceProvider.GetRequiredService<IDatasetReader<TestEntity>>();

        // Access via alias reader (keyed)
        var aliasReader = serviceProvider.GetDatasetReader<TestEntity>("my-aliases");

        // Assert
        directReader.Should().NotBeNull("direct dataset should be available");
        aliasReader.Should().NotBeNull("alias reader should be available");
        directReader.Should().Be(dataset, "direct reader should be the dataset itself");

        // Both should work, but in different ways
        // Direct reader requires knowing the primary ID
        var directResult = await directReader!.Get("relation/3");
        directResult.Should().NotBeNull("direct access by primary ID should work");

        // Alias reader allows access by alias
        var aliasResult = await aliasReader!.Get("alias-name");
        aliasResult.Should().NotBeNull("alias access should work");

        // Both should return the same entity
        var directEntity = TestEntity.Parser.ParseFrom(directResult!.Value.entityBytes.Span);
        var aliasEntity = TestEntity.Parser.ParseFrom(aliasResult!.Value.entityBytes.Span);

        directEntity.Id.Should().Be("relation/3");
        aliasEntity.Id.Should().Be("relation/3");
    }

    [Fact]
    public async Task Should_Retrieve_Correct_Reader_When_Multiple_Are_Registered()
    {
        // Arrange - set up DI container with multiple alias readers
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        // Register three different alias readers
        services.AddDatasetAlias<TestEntity>("reader-1", ExtractAliases);
        services.AddDatasetAlias<TestEntity>("reader-2", ExtractAliases);
        services.AddDatasetAlias<TestEntity>("reader-3", ExtractAliases);

        var serviceProvider = services.BuildServiceProvider();

        // Create test data
        var relation = new TestEntity { Id = "relation/4" };
        relation.Aliases.Add("test-alias");
        await dataset.Put(relation.Id, relation.ToByteArray(), null, null);

        // Act - retrieve each reader by name
        var reader1 = serviceProvider.GetDatasetReader<TestEntity>("reader-1");
        var reader2 = serviceProvider.GetDatasetReader<TestEntity>("reader-2");
        var reader3 = serviceProvider.GetDatasetReader<TestEntity>("reader-3");
        var readerNonexistent = serviceProvider.GetDatasetReader<TestEntity>("reader-nonexistent");

        // Assert
        reader1.Should().NotBeNull("reader-1 should be registered");
        reader2.Should().NotBeNull("reader-2 should be registered");
        reader3.Should().NotBeNull("reader-3 should be registered");
        readerNonexistent.Should().BeNull("nonexistent reader should return null");

        reader1.Should().NotBeSameAs(reader2, "different readers should be different instances");
        reader2.Should().NotBeSameAs(reader3, "different readers should be different instances");
        reader1.Should().NotBeSameAs(reader3, "different readers should be different instances");

        // All should work
        var result1 = await reader1!.Get("test-alias");
        var result2 = await reader2!.Get("test-alias");
        var result3 = await reader3!.Get("test-alias");

        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result3.Should().NotBeNull();

        var entity1 = TestEntity.Parser.ParseFrom(result1!.Value.entityBytes.Span);
        var entity2 = TestEntity.Parser.ParseFrom(result2!.Value.entityBytes.Span);
        var entity3 = TestEntity.Parser.ParseFrom(result3!.Value.entityBytes.Span);

        entity1.Id.Should().Be("relation/4");
        entity2.Id.Should().Be("relation/4");
        entity3.Id.Should().Be("relation/4");
    }

    [Fact]
    public void GetAliasesFor_Returns_All_Tuples_For_All_Registered_Alias_Names()
    {
        // Arrange
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        services.AddDatasetAlias<TestEntity>("byitem", ExtractAliases);
        services.AddDatasetAlias<TestEntity>("bybarcode", bytes =>
        {
            var entity = TestEntity.Parser.ParseFrom(bytes.Span);
            return entity.Aliases.Select(a => "barcode:" + a);
        });

        var sp = services.BuildServiceProvider();

        var dto = new TestEntity { Id = "relation/5" };
        dto.Aliases.Add("item-abc");

        // Act
        var aliases = sp.GetAliasesFor("relation/5", dto);

        // Assert
        aliases.Should().HaveCount(2);
        aliases.Should().Contain(("byitem", "item-abc"));
        aliases.Should().Contain(("bybarcode", "barcode:item-abc"));
    }

    [Fact]
    public void GetAliasesFor_Returns_Empty_When_No_Alias_Readers_Registered()
    {
        // Arrange
        var services = new ServiceCollection();
        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);

        var sp = services.BuildServiceProvider();
        var dto = new TestEntity { Id = "relation/6" };

        // Act
        var aliases = sp.GetAliasesFor("relation/6", dto);

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void GetAliasesFor_Returns_Empty_When_Factory_Produces_No_Aliases()
    {
        // Arrange
        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        services.AddDatasetAlias<TestEntity>("empty-aliases", _ => Enumerable.Empty<string>());

        var sp = services.BuildServiceProvider();
        var dto = new TestEntity { Id = "relation/7" };

        // Act
        var aliases = sp.GetAliasesFor("relation/7", dto);

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact(Skip="NotRelevant")]
    public void GetAliasesFor_Passes_EntityId_To_Factory()
    {
        // Arrange
        string? capturedEntityId = null;

        var services = new ServiceCollection();
        var logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        var dataset = new InMemoryDataset<TestEntity>();
        services.AddSingleton<IDatasetReader<TestEntity>>(dataset);
        services.AddSingleton(logger);
        services.AddSingleton<IHostApplicationLifetime, DummyHostApplicationLifetime>();

        services.AddDatasetAlias<TestEntity>("id-capture", (_) =>
        {
            capturedEntityId = "";//entityId;
            return ["captured"];
        });

        var sp = services.BuildServiceProvider();
        var dto = new TestEntity { Id = "relation/8" };

        // Act
        sp.GetAliasesFor("relation/8", dto);

        // Assert
        capturedEntityId.Should().Be("relation/8");
    }

    // Dummy implementation for IHostApplicationLifetime for testing purposes
    private class DummyHostApplicationLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() { }
    }
}
