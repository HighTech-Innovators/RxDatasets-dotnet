using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

public class DataReaderCountChildrenTests
{
    private const string ParentPath = "departments/engineering";
    private const string ChildName1 = "departments/engineering/employees/alice";
    private const string ChildName2 = "departments/engineering/employees/bob";
    private const string ChildName3 = "departments/engineering/employees/charlie";
    
    private static readonly Demodto ChildEntity1 = new() { Name = ChildName1 };
    private static readonly Demodto ChildEntity2 = new() { Name = ChildName2 };
    private static readonly Demodto ChildEntity3 = new() { Name = ChildName3 };

    private readonly GrpcReaderServiceImpl<Demodto> _grpcService;
    private readonly InMemoryDataset<Demodto> _dataset;

    public DataReaderCountChildrenTests()
    {
        _dataset = new InMemoryDataset<Demodto>();
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        var cacheDuration = "1";
        configurationSection.Value.Returns(cacheDuration);
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        _grpcService = new GrpcReaderServiceImpl<Demodto>(
            Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(),
            configuration,
            _dataset);
    }

    [Fact]
    public async Task CountChildren_WithParentHavingChildren_ReturnsCorrectCount()
    {
        // Given
        await _dataset.Put(ChildName1, ChildEntity1.ToByteArray());
        await _dataset.Put(ChildName2, ChildEntity2.ToByteArray());
        await _dataset.Put(ChildName3, ChildEntity3.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(ParentPath, CancellationToken.None);

        // Then
        count.Should().Be(3);
    }

    [Fact]
    public async Task CountChildren_WithParentHavingNoChildren_ReturnsZero()
    {
        // Given - no entities added for this parent
        const string emptyParentPath = "departments/hr";

        // When
        var count = await _grpcService.CountChildren(emptyParentPath, CancellationToken.None);

        // Then
        count.Should().Be(0);
    }

    [Fact]
    public async Task CountChildren_WithNonExistentParent_ReturnsZero()
    {
        // Given
        const string nonExistentParent = "departments/nonexistent";

        // When
        var count = await _grpcService.CountChildren(nonExistentParent, CancellationToken.None);

        // Then
        count.Should().Be(0);
    }

    [Fact]
    public async Task CountChildren_WithCancellationToken_RespectsCancellation()
    {
        // Given
        await _dataset.Put(ChildName1, ChildEntity1.ToByteArray());
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // When & Then
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _grpcService.CountChildren(ParentPath, cts.Token));
    }

    [Fact]
    public async Task CountChildren_WithRootPath_CountsAllTopLevelEntities()
    {
        // Given
        const string rootPath = "";
        const string topLevel1 = "department1/info";
        const string topLevel2 = "department2/info";
        const string nested = "department1/team/member";

        await _dataset.Put(topLevel1, ChildEntity1.ToByteArray());
        await _dataset.Put(topLevel2, ChildEntity2.ToByteArray());
        await _dataset.Put(nested, ChildEntity3.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(rootPath, CancellationToken.None);

        // Then - should count all descendants under root
        count.Should().Be(3);
    }

    [Fact]
    public async Task CountChildren_WithManyChildren_ReturnsAccurateCount()
    {
        // Given
        const int childCount = 100;
        for (int i = 0; i < childCount; i++)
        {
            var childName = $"departments/engineering/employees/employee{i}";
            var entity = new Demodto { Name = childName };
            await _dataset.Put(childName, entity.ToByteArray());
        }

        // When
        var count = await _grpcService.CountChildren(ParentPath, CancellationToken.None);

        // Then
        count.Should().Be(childCount);
    }

    [Fact]
    public async Task CountChildren_PerformanceValidation_CompletesWithin100msFor10000Children()
    {
        // Given - populate dataset with 10,000 children
        const int childCount = 10000;
        const string parentPath = "org/teams/developers";
        
        for (int i = 0; i < childCount; i++)
        {
            var childName = $"{parentPath}/member{i}";
            var entity = new Demodto { Name = childName };
            await _dataset.Put(childName, entity.ToByteArray());
        }

        // When - measure performance
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var count = await _grpcService.CountChildren(parentPath, CancellationToken.None);
        stopwatch.Stop();

        // Then - verify accuracy and performance
        count.Should().Be(childCount);
        stopwatch.ElapsedMilliseconds.Should().BeLessThan(100, 
            because: "CountChildren should complete within 100ms for 10,000 children");
    }

    // Edge Case Tests (T033)
    [Fact]
    public async Task CountChildren_WithSpecialCharactersInPath_ReturnsCorrectCount()
    {
        // Given - paths with special characters
        const string parentPath = "department-team_01/sub.section";
        const string childWithSpecialChars1 = "department-team_01/sub.section/resource-1";
        const string childWithSpecialChars2 = "department-team_01/sub.section/resource_2";
        
        var entity1 = new Demodto { Name = childWithSpecialChars1 };
        var entity2 = new Demodto { Name = childWithSpecialChars2 };
        
        await _dataset.Put(childWithSpecialChars1, entity1.ToByteArray());
        await _dataset.Put(childWithSpecialChars2, entity2.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(parentPath, CancellationToken.None);

        // Then
        count.Should().Be(2);
    }

    [Fact]
    public async Task CountChildren_WithUnicodeCharactersInPath_ReturnsCorrectCount()
    {
        // Given - paths with unicode characters
        const string parentPath = "部門/チーム";
        const string childWithUnicode1 = "部門/チーム/メンバー1";
        const string childWithUnicode2 = "部門/チーム/メンバー2";
        const string childWithUnicode3 = "部門/チーム/成員3";
        
        var entity1 = new Demodto { Name = childWithUnicode1 };
        var entity2 = new Demodto { Name = childWithUnicode2 };
        var entity3 = new Demodto { Name = childWithUnicode3 };
        
        await _dataset.Put(childWithUnicode1, entity1.ToByteArray());
        await _dataset.Put(childWithUnicode2, entity2.ToByteArray());
        await _dataset.Put(childWithUnicode3, entity3.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(parentPath, CancellationToken.None);

        // Then
        count.Should().Be(3);
    }

    [Fact]
    public async Task CountChildren_WithVeryLongPath_ReturnsCorrectCount()
    {
        // Given - very long path (over 260 characters)
        var longPath = string.Join("/", Enumerable.Range(0, 30).Select(i => $"segment{i:D2}"));
        var childPath1 = $"{longPath}/child1";
        var childPath2 = $"{longPath}/child2";
        
        var entity1 = new Demodto { Name = childPath1 };
        var entity2 = new Demodto { Name = childPath2 };
        
        await _dataset.Put(childPath1, entity1.ToByteArray());
        await _dataset.Put(childPath2, entity2.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(longPath, CancellationToken.None);

        // Then
        count.Should().Be(2);
    }

    [Fact]
    public async Task CountChildren_WithWhitespaceInPath_ReturnsCorrectCount()
    {
        // Given - paths with whitespace
        const string parentPath = "department name/team section";
        const string childWithSpace1 = "department name/team section/resource 1";
        const string childWithSpace2 = "department name/team section/resource 2";
        
        var entity1 = new Demodto { Name = childWithSpace1 };
        var entity2 = new Demodto { Name = childWithSpace2 };
        
        await _dataset.Put(childWithSpace1, entity1.ToByteArray());
        await _dataset.Put(childWithSpace2, entity2.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(parentPath, CancellationToken.None);

        // Then
        count.Should().Be(2);
    }

    [Fact]
    public async Task CountChildren_CountsAllDescendants_IncludingGrandchildren()
    {
        // Given - multiple hierarchy levels
        const string parentPath = "org/division";
        const string directChild1 = "org/division/team1";
        const string directChild2 = "org/division/team2";
        const string sibling = "org/otherDivision";
        const string grandchild = "org/division/team1/member";
        
        var entity1 = new Demodto { Name = directChild1 };
        var entity2 = new Demodto { Name = directChild2 };
        var entity3 = new Demodto { Name = sibling };
        var entity4 = new Demodto { Name = grandchild };
        
        await _dataset.Put(directChild1, entity1.ToByteArray());
        await _dataset.Put(directChild2, entity2.ToByteArray());
        await _dataset.Put(sibling, entity3.ToByteArray());
        await _dataset.Put(grandchild, entity4.ToByteArray());

        // When
        var count = await _grpcService.CountChildren(parentPath, CancellationToken.None);

        // Then - should count all descendants (direct children and grandchildren) under parent, but not siblings
        count.Should().Be(3);
    }

    [Fact]
    public async Task CountChildren_WithEmptyString_CountsAllDescendantsAtRoot()
    {
        // Given
        const string topLevel1 = "department/info";
        const string topLevel2 = "team/info";
        const string nested = "department/team/member";
        
        var entity1 = new Demodto { Name = topLevel1 };
        var entity2 = new Demodto { Name = topLevel2 };
        var entity3 = new Demodto { Name = nested };
        
        await _dataset.Put(topLevel1, entity1.ToByteArray());
        await _dataset.Put(topLevel2, entity2.ToByteArray());
        await _dataset.Put(nested, entity3.ToByteArray());

        // When - test with empty string parent (should treat as root, count all descendants)
        var countWithEmpty = await _grpcService.CountChildren(string.Empty, CancellationToken.None);

        // Then - CountChildren always counts all descendants, not just direct children
        countWithEmpty.Should().Be(3);
    }
}
