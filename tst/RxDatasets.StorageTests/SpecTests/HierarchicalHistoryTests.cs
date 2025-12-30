using System.Collections.Immutable;
using FluentAssertions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageTests.SpecTests;

public class HierarchicalHistoryTests
{
    [Fact]
    public async Task StreamHistory_ShouldReturnFullPaths_WhenReadingFromHierarchicalDelta()
    {
        // Arrange
        var storageMock = new Mock<IReadOnlyStorageProvider<object>>();
        var logger = Mock.Of<ILogger<DatasetReader<object>>>();
        
        // Setup version info
        var snapshotInfo = new SnapshotInfo();
        snapshotInfo.SnapshotVersions.Add(new SnapshotInfo.Types.snapshotVersion { Version = 1, Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow) });
        var deltaInfo = new DeltaInfo { EarliestDeltaVersion = 1, LatestDeltaVersion = 2 };
        
        storageMock.Setup(x => x.ListVersions())
            .ReturnsAsync((deltaInfo.EarliestDeltaVersion, deltaInfo.LatestDeltaVersion, 
                snapshotInfo.SnapshotVersions.Select(s => new SnapshotType(s.Version, s.Timestamp.ToDateTimeOffset())).ToList()));

        // Setup Snapshot 1 (empty)
        var snapshotIndex = new SnapshotIndex { SnapshotVersion = 1 };
        storageMock.Setup(x => x.ReadSnapshotIndex(1))
            .ReturnsAsync(new ReadOnlyMemory<byte>(snapshotIndex.ToByteArray()));
        storageMock.Setup(x => x.ReadSnapshotData(1))
            .ReturnsAsync(new ReadOnlyMemory<byte>([]));
        storageMock.Setup(x => x.GetSnapshotDataContentLength(1))
            .ReturnsAsync(0);

        // Setup Delta 2 (Hierarchical)
        // Structure: A/B/C
        var deltaNode = new DeltaNode();
        var nodeA = new DeltaNode();
        var nodeB = new DeltaNode();
        
        var entityC = new DeltaEntity { Name = "C", PreviousVersion = 0, Position = 0, Length = 10 };
        nodeB.NewEntities.Add("C", entityC);
        
        nodeA.Children.Add("B", nodeB);
        deltaNode.Children.Add("A", nodeA);

        var deltaIndex = new DeltaIndex
        {
            DeltaVersion = 2,
            Root = deltaNode,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };

        storageMock.Setup(x => x.ReadDeltaIndex(2))
            .ReturnsAsync(new ReadOnlyMemory<byte>(deltaIndex.ToByteArray()));

        var reader = new DatasetReader<object>(logger, storageMock.Object, null);
        await reader.StartAsync(CancellationToken.None);

        // Act
        var history = await reader.StreamHistory("A").ToListAsync();

        // Assert
        history.Should().HaveCount(1);
        history[0].Id.Should().Be("A/B/C");
    }

    [Fact]
    public async Task GetHistoric_ShouldReturnData_WhenReadingFromHierarchicalDelta()
    {
        // Arrange
        var storageMock = new Mock<IReadOnlyStorageProvider<object>>();
        var logger = Mock.Of<ILogger<DatasetReader<object>>>();
        
        // Setup version info
        var snapshotInfo = new SnapshotInfo();
        snapshotInfo.SnapshotVersions.Add(new SnapshotInfo.Types.snapshotVersion { Version = 1, Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow) });
        var deltaInfo = new DeltaInfo { EarliestDeltaVersion = 1, LatestDeltaVersion = 2 };
        
        storageMock.Setup(x => x.ListVersions())
            .ReturnsAsync((deltaInfo.EarliestDeltaVersion, deltaInfo.LatestDeltaVersion, 
                snapshotInfo.SnapshotVersions.Select(s => new SnapshotType(s.Version, s.Timestamp.ToDateTimeOffset())).ToList()));

        // Setup Snapshot 1 (empty)
        var snapshotIndex = new SnapshotIndex { SnapshotVersion = 1 };
        storageMock.Setup(x => x.ReadSnapshotIndex(1))
            .ReturnsAsync(new ReadOnlyMemory<byte>(snapshotIndex.ToByteArray()));
        storageMock.Setup(x => x.ReadSnapshotData(1))
            .ReturnsAsync(new ReadOnlyMemory<byte>([]));
        storageMock.Setup(x => x.GetSnapshotDataContentLength(1))
            .ReturnsAsync(0);

        // Setup Delta 2 (Hierarchical)
        // Structure: A/B/C
        var deltaNode = new DeltaNode();
        var nodeA = new DeltaNode();
        var nodeB = new DeltaNode();
        
        var entityC = new DeltaEntity { Name = "C", PreviousVersion = 0, Position = 100, Length = 10 };
        nodeB.NewEntities.Add("C", entityC);
        
        nodeA.Children.Add("B", nodeB);
        deltaNode.Children.Add("A", nodeA);

        var deltaIndex = new DeltaIndex
        {
            DeltaVersion = 2,
            Root = deltaNode,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };

        storageMock.Setup(x => x.ReadDeltaIndex(2))
            .ReturnsAsync(new ReadOnlyMemory<byte>(deltaIndex.ToByteArray()));

        // Setup data read
        var expectedData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        storageMock.Setup(x => x.ReadDataFromSources(
                It.Is<List<(ObjectNameTypeEnum, ulong, long)>>(l => 
                    l.Count == 1 && 
                    l[0].Item1 == ObjectNameTypeEnum.Delta && 
                    l[0].Item2 == 2 && 
                    l[0].Item3 == 100), 
                10))
            .ReturnsAsync(new ReadOnlyMemory<byte>(expectedData));

        var reader = new DatasetReader<object>(logger, storageMock.Object, null);
        await reader.StartAsync(CancellationToken.None);

        // Act
        var result = await reader.GetHistoric("A/B/C", 2);

        // Assert
        result.Should().NotBeNull();
        result!.Value.name.Should().Be("A/B/C");
        result.Value.entityBytes.ToArray().Should().BeEquivalentTo(expectedData);
    }
}
