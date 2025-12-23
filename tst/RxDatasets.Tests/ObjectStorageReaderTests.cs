using System.IO;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

public class ObjectStorageReaderTests
{
    [Fact]
    public async Task ListVersions_RebuildsFromListingWhenLatestDeltaIsMissing()
    {
        // Arrange
        var storageConfig = new StorageConfig { DtoName = "testdto" };
        var options = Options.Create(storageConfig);
        var diskStorage = Substitute.For<IDiskStorage<TestDto>>();
        var logger = Substitute.For<ILogger<ObjectStorageReader<TestDto>>>();
        var sut = new ObjectStorageReader<TestDto>(logger, options, diskStorage);

        var prefix = DatasetHelper.GetObjectNamePrefix<TestDto>(storageConfig);
        var deltaInfoFile = prefix + "DeltaInfo.pb";
        var snapshotInfoFile = prefix + "SnapshotInfo.pb";

        var deltaInfo = new DeltaInfo { EarliestDeltaVersion = 5, LatestDeltaVersion = 5 };
        var snapshotInfo = new SnapshotInfo();
        var deltaBytes = new ReadOnlyMemory<byte>(deltaInfo.ToByteArray());
        var snapshotBytes = new ReadOnlyMemory<byte>(snapshotInfo.ToByteArray());

        diskStorage.DownloadBytes(deltaInfoFile)
            .Returns(Task.FromResult(deltaBytes));
        diskStorage.DownloadBytes(snapshotInfoFile)
            .Returns(Task.FromResult(snapshotBytes));

        var missingIndexPath = DatasetHelper.GetObjectName<TestDto>(deltaInfo.LatestDeltaVersion,
            ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta, storageConfig);

        diskStorage.GetObjectContentLength(missingIndexPath)
            .Returns<Task<long>>(_ => throw new FileNotFoundException());

        var now = DateTimeOffset.UtcNow;
        var deltaListing = new List<(string ObjectKey, DateTimeOffset DateTime)>
        {
            ($"{prefix}{FolderNames.DeltasFolderName}/3/data.pbs", now.AddMinutes(-2)),
            ($"{prefix}{FolderNames.DeltasFolderName}/4/data.pbs", now.AddMinutes(-1))
        };
        diskStorage.ListObjects(prefix + FolderNames.DeltasFolderNameWithSlash)
            .Returns(Task.FromResult(deltaListing));

        var snapshotListing = new List<(string ObjectKey, DateTimeOffset DateTime)>
        {
            ($"{prefix}{FolderNames.SnapshotsFolderName}/2/data.pbs", now)
        };
        diskStorage.ListObjects(prefix + FolderNames.SnapshotsFolderNameWithSlash)
            .Returns(Task.FromResult(snapshotListing));

        // Act
        var versions = await sut.ListVersions();

        // Assert
        versions.OldestDelta.Should().Be(3);
        versions.NewestDelta.Should().Be(4);
        var snapshot = versions.Snapshots.Should().ContainSingle().Subject;
        snapshot.Version.Should().Be(2);
    }

    public sealed class TestDto
    {
    }
}
