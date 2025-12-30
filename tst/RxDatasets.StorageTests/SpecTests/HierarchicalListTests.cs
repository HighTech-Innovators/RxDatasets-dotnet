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

public class HierarchicalListTests
{
    [Fact]
    public async Task List_ShouldReturnRecursiveEntities_WhenListingDirectory()
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
        // Structure: 
        // t/guid/s/file3 (file inside folder s)
        // t/guid/file1 (file)
        // t/guid/file2 (file)
        
        var deltaNode = new DeltaNode();
        var nodeT = new DeltaNode();
        var nodeGuid = new DeltaNode();
        var nodeS = new DeltaNode();
        
        // Add 'file3' to 's'
        var entity3 = new DeltaEntity { Name = "file3", PreviousVersion = 0, Position = 0, Length = 10 };
        nodeS.NewEntities.Add("file3", entity3);

        // Add 's' as a child folder of 'guid'
        nodeGuid.Children.Add("s", nodeS);
        
        // Add 'file1' and 'file2' as entities in 'guid'
        var entity1 = new DeltaEntity { Name = "file1", PreviousVersion = 0, Position = 0, Length = 10 };
        var entity2 = new DeltaEntity { Name = "file2", PreviousVersion = 0, Position = 0, Length = 10 };
        
        nodeGuid.NewEntities.Add("file1", entity1);
        nodeGuid.NewEntities.Add("file2", entity2);
        
        nodeT.Children.Add("guid", nodeGuid);
        deltaNode.Children.Add("t", nodeT);

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
        var (names, _) = await reader.List("t/guid");

        // Assert
        names.Should().Contain("t/guid/s/file3");
        names.Should().Contain("t/guid/file1");
        names.Should().Contain("t/guid/file2");
        names.Should().NotContain("t/guid/s"); // Should not contain the folder itself
        names.Should().HaveCount(3);
    }
}
