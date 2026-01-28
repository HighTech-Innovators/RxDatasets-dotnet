using FluentAssertions;

namespace RxDatasets.StorageTests.Common;

public abstract class StorageTestSpecWithTwoStepInit : StorageTestSpec
{
    [Fact]
    public async Task Should_Initialize_Index_With_SnapShot()
    {
        var cancallationSource = new CancellationTokenSource();
        var (datasetReader, datasetWriter) = await SetupStorageAsync(cancallationSource.Token);
        var datasetVersion = datasetReader.GetLatestVersion();

        var data = GenerateRandomData(UNIT_KB);
        await datasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data, false, cancallationSource.Token);
        datasetVersion++;

        //Add new entries until the next snapshot creation
        while (datasetVersion < 25) // 1,11,16,21
        {
            await datasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data, false, cancallationSource.Token);
            datasetVersion++;
        }

        await Task.Delay(TimeSpan.FromSeconds(0.1), cancallationSource.Token);
        await cancallationSource.CancelAsync();
        cancallationSource.TryReset();
        //Create a new Dataset instance, they should return identical results
        var (newReader, _) = await SetupStorageAsync(cancallationSource.Token);
        var datasetVersionOldDataset = datasetReader.GetLatestVersion();
        var datasetSnapshotVersionOldDataset = datasetReader.GetLastUsedSnapshotVersion();
        var datasetVersionNewDataset = newReader.GetLatestVersion();
        var datasetSnapshotVersionNewDataset = newReader.GetMainIndexSnapshotVersion();

        datasetVersionNewDataset.Should().Be(datasetVersionOldDataset);
        datasetSnapshotVersionNewDataset.Should().Be(datasetSnapshotVersionOldDataset);

        for (ulong i = 0; i < datasetVersion; i++)
        {
            var bytesOldDataset = await datasetReader.Get($"Should_Initialize_Index_With_SnapShot_{i}");
            var bytesNewDataset = await newReader.Get($"Should_Initialize_Index_With_SnapShot_{i}");

            bytesOldDataset.Should().NotBeNull();
            bytesNewDataset.Should().NotBeNull();

            bytesOldDataset.Value.name.Should().BeEquivalentTo(bytesNewDataset.Value.name);
            bytesNewDataset.Value.entityBytes.ToArray().Should().BeEquivalentTo(bytesOldDataset.Value.entityBytes.ToArray());
        }
        cancallationSource.Dispose();
    }

    [Fact]
    public async Task Should_Initialize_Index_Delta()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var datasetVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var snapshotInterval = SnapshotsOptions.Value.SnapshotInterval;

        var data = GenerateRandomData(UNIT_KB);

        //Add new entries until the next snapshot creation
        while (datasetVersion < 25)
        {
            datasetVersion++;
            await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);
        }

        //Now do 1 more, so we for sure have 1 put taking place after the snapshot
        datasetVersion++;
        await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);

        //Create a new Dataset instance, they should return identical results
        var (newReader, newWriter) = await SetupStorageAsync(CancellationToken.None);
        var datasetVersionNewDataset = newReader.GetLatestVersion();

        datasetVersionNewDataset.Should().Be(datasetVersion);

        var bytesOldDataset = await azBlobStorageDatasetReader.Get($"Should_Initialize_Index_With_SnapShot_{datasetVersion}");
        var bytesNewDataset = await newReader.Get($"Should_Initialize_Index_With_SnapShot_{datasetVersion}");

        bytesNewDataset.Should().NotBeNull();
        bytesNewDataset.Value.entityBytes.ToArray().Should().BeEquivalentTo(bytesOldDataset.Value.entityBytes.ToArray());
    }
}
