using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FluentAssertions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;
using RxDatasets.StorageTests.Services;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;

public partial class UsingLowLevelInMemoryStorage : StorageTestSpecWithTwoStepInit
{
    private readonly LowLevelInMemoryStorage<object> _storage;

    public UsingLowLevelInMemoryStorage()
    {
        var storageLogger = new Mock<ILogger<LowLevelInMemoryStorage<object>>>();

        _storage = new LowLevelInMemoryStorage<object>(storageLogger.Object);
    }

    internal static IHostedService CreateSnapshotCreationService(IStorageProvider<object> storageProvider, INotificationsHub<object> notificationsHub)
    {
        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        var snapshotCreationConfig = Options.Create(new SnapshotCreationConfig { SnapshotInterval = 5 });
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, storageProvider, snapshotCreationConfig, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(datasetUpdateConsumingServiceLogger.Object, notificationsHub, snapshotCreationPlugin);
        return datasetUpdateConsumingService;
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, _storage);

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, _storage, notificationsHub, readOnlyStorage, false);

        var scs = CreateSnapshotCreationService(persistentStorageProvider, notificationsHub);

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, notificationsHub);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig()), false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);
        await scs.StartAsync(CancellationToken.None);
        await datasetReader.StartAsync(CancellationToken.None);

        return (datasetReader, datasetWriter);
    }

    [Fact]
    public async Task Should_Initialize_Index_With__SnapShot()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Act
        await CreateNumberDeltasWithOffset(datasetWriter, 0, 6);
        await WaitForSnapshotVersionsAtLeast(1);
        await CreateNumberDeltasWithOffset(datasetWriter, 6, 5);
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var latestSnapshot = snapshotVersions[^1];

        // Assert
        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(latestSnapshot, "Create a new Dataset instance, they should return identical results");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_SnapShot_That_Only_Contains_Deleted_Entities()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Act — first delta triggers a time-based snapshot at v1, so we need gap of 5 from v1
        // Write 5 deltas (v1-v5, snapshot at v1), then PutAll at v6 (gap 6-1=5 → snapshot with all deleted)
        await CreateNumberDeltasWithOffset(datasetWriter, 0, 5);
        await WaitForSnapshotVersionsAtLeast(1);
        await datasetWriter.PutAll(AsyncEnumerable.Empty<(string, byte[])>());
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var deleteAllSnapshot = snapshotVersions[^1];

        // Assert — the snapshot should only contain deleted entities (1-byte sentinel)
        var snapshotObjectName = DatasetHelper.GetObjectName<object>(deleteAllSnapshot, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, new StorageConfig());
        var actualSnapshotLength = await _storage.GetObjectContentLength(snapshotObjectName);
        actualSnapshotLength.Should().Be(1);

        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(deleteAllSnapshot, "Create a new Dataset instance, they should return identical results");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_SnapShot_That_Only_Contains_Deleted_Entities_Even_If_The_Snapshot_Data_Is_Absent()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Act — first delta triggers a time-based snapshot at v1, so we need gap of 5 from v1
        await CreateNumberDeltasWithOffset(datasetWriter, 0, 5);
        await WaitForSnapshotVersionsAtLeast(1);
        await datasetWriter.PutAll(AsyncEnumerable.Empty<(string, byte[])>());
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var deleteAllSnapshot = snapshotVersions[^1];

        await _storage.DeleteFile(DatasetHelper.GetObjectName<object>(deleteAllSnapshot, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, new StorageConfig()));

        // Assert
        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(deleteAllSnapshot, "Create a new Dataset instance, they should return identical results");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Index_Does_Not_Exist()
    {
        // Arrange
        var (datasetReader, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        await CreateNumberDeltasWithOffset(datasetWriter, 0, 6);
        await WaitForSnapshotVersionsAtLeast(1);

        await CreateNumberDeltasWithOffset(datasetWriter, 6, 5);
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var latestSnapshotVersion = snapshotVersions[^1];
        var previousSnapshotVersion = snapshotVersions[^2];

        datasetReader.GetLastUsedSnapshotVersion().Should().Be(latestSnapshotVersion, "Reader should use latest available snapshot");

        // Act
        await _storage.DeleteFile(DatasetHelper.GetObjectName<object>(latestSnapshotVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot, new StorageConfig()));

        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(previousSnapshotVersion, "Because we removed the last snapshot index data");
        toTestReader.GetLatestVersion().Should().Be(11, "We should read all the deltas after the earlier snapshot");

        var changes = await toTestReader.GetChangedNames(0);
        changes.Changes.Where(ch => ch.ChangeType == ChangeType.New).Should().HaveCount(11, "We should observe all 11 insertions, so there is no possibility of lying about versions based on assumptions from the published snapshot version");
    }

    private static async Task CreateNumberDeltasWithOffset(DatasetWriter<object> datasetWriter, ulong offset, ulong count)
    {
        for (var i = 0ul; i < count; i++)
        {
            var version = offset + i;
            await datasetWriter.Put($"Same_DTO{version}", BitConverter.GetBytes(version));
        }
    }
    private async Task<List<ulong>> WaitForSnapshotVersionsAtLeast(int minimumSnapshots)
    {
        var snapshotPrefix = DatasetHelper.GetObjectNamePrefix<object>(new StorageConfig()) + FolderNames.SnapshotsFolderNameWithSlash;

        for (var i = 0; i < 50; i++)
        {
            var snapshotVersions = (await _storage.ListObjects(snapshotPrefix))
                .Select(s => (ulong)SharedMethods.ExtractNumber(s.ObjectKey))
                .Where(v => v > 0)
                .Distinct()
                .OrderBy(v => v)
                .ToList();

            if (snapshotVersions.Count >= minimumSnapshots)
                return snapshotVersions;

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        var latestSnapshotVersions = (await _storage.ListObjects(snapshotPrefix))
            .Select(s => (ulong)SharedMethods.ExtractNumber(s.ObjectKey))
            .Where(v => v > 0)
            .Distinct()
            .OrderBy(v => v)
            .ToList();

        Assert.Fail($"Expected at least {minimumSnapshots} snapshots, got {latestSnapshotVersions.Count}");
        return latestSnapshotVersions;
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Data_Does_Not_Exist()
    {
        // Arrange
        var (datasetReader, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        await CreateNumberDeltasWithOffset(datasetWriter, 0, 6);
        await WaitForSnapshotVersionsAtLeast(1);

        await CreateNumberDeltasWithOffset(datasetWriter, 6, 5);
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var latestSnapshotVersion = snapshotVersions[^1];
        var previousSnapshotVersion = snapshotVersions[^2];

        datasetReader.GetLastUsedSnapshotVersion().Should().Be(latestSnapshotVersion, "Reader should use latest available snapshot");

        // Act
        await _storage.DeleteFile(DatasetHelper.GetObjectName<object>(latestSnapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, new StorageConfig()));

        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(previousSnapshotVersion, "Because we removed the last snapshot data");
        toTestReader.GetLatestVersion().Should().Be(11, "We should read all the deltas after the earlier snapshot");

        var changes = await toTestReader.GetChangedNames(0);
        changes.Changes.Where(ch => ch.ChangeType == ChangeType.New).Should().HaveCount(11, "We should observe all 11 insertions, so there is no possibility of lying about versions based on assumptions from the published snapshot version");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Does_Exist_But_Is_Corrupted()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        await CreateNumberDeltasWithOffset(datasetWriter, 0, 6);
        await WaitForSnapshotVersionsAtLeast(1);

        await CreateNumberDeltasWithOffset(datasetWriter, 6, 5);
        var snapshotVersions = await WaitForSnapshotVersionsAtLeast(2);
        var latestSnapshotVersion = snapshotVersions[^1];
        var previousSnapshotVersion = snapshotVersions[^2];

        // Act
        var snapshotObjectName = DatasetHelper.GetObjectName<object>(latestSnapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, new StorageConfig());
        var actualSnapshotLength = await _storage.GetObjectContentLength(snapshotObjectName);
        var wrongBytes = new byte[actualSnapshotLength - 1]; // This should fail since it short
        await _storage.UploadBytes(snapshotObjectName, wrongBytes);

        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(previousSnapshotVersion, "Because we corrupted the last snapshot data");
    }

    private async Task<IStorageProvider<object>> SetupProviderAsync(UploadVerificationMode verificationMode,
                                                                    IDiskStorage<object> lowLevelStorageProvider)
    {
        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        StorageConfig.Value.UploadVerificationMode = verificationMode;
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, lowLevelStorageProvider);

        var persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, lowLevelStorageProvider, notificationsHub, readOnlyStorage, false);
        await persistentStorageProvider.StartAsync(CancellationToken.None);

        return persistentStorageProvider;
    }

    [Fact]
    public async Task Verify_Data_Existence_After_Upload_With_IncorrectData()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);

        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).ReturnsAsync(bytes.Length - 1);
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).ReturnsAsync(bytes.Length - 1);

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, bytes);
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().ThrowAsync<ExternalException>();
        await deltaStoreAction.Should().ThrowAsync<ExternalException>();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Once);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Once);
    }

    [Fact]
    public async Task DoNot_Verify_Data_Existence_After_Upload_EvenWithIncorrectData()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.DoNotVerify, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).ReturnsAsync(bytes.Length - 1);
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).ReturnsAsync(bytes.Length - 1);

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, bytes);
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().NotThrowAsync();
        await deltaStoreAction.Should().NotThrowAsync();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Never);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Never);
    }

    [Fact]
    public async Task VerifyData_Existence_AfterUpload_WithNotFoundData()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).Throws(new FileNotFoundException());
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).Throws(new FileNotFoundException());

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, bytes);
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().ThrowAsync<ExternalException>();
        await deltaStoreAction.Should().ThrowAsync<ExternalException>();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Once);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Once);
    }

    [Fact]
    public async Task VerifyDataExistence_Always_AfterUpload()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).ReturnsAsync(bytes.Length);
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).ReturnsAsync(bytes.Length);

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, bytes);
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().NotThrowAsync();
        await deltaStoreAction.Should().NotThrowAsync();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Once);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Once);
    }

    [Fact]
    public async Task VerifyData_Existence_ForStreamsOnly_AfterUploadingCorrectly()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyLargeDataOnly, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).ReturnsAsync(bytes.Length);
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).ReturnsAsync(bytes.Length);

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, new MemoryStream(bytes));
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().NotThrowAsync();
        await deltaStoreAction.Should().NotThrowAsync();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Once);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Never);
    }

    [Fact]
    public async Task Verify_Data_Existence_ForStreams_OnlyAfterUpload_WithCorruptedFiles()
    {
        var lowLevelStorageProvider = new Mock<IDiskStorage<object>>();
        lowLevelStorageProvider.Setup(m => m.ListObjects(It.IsAny<string>())).ReturnsAsync([]);

        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyLargeDataOnly, lowLevelStorageProvider.Object);

        var snapshotDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, StorageConfig.Value);
        var deltaDataObjectName = DatasetHelper.GetObjectName<object>(1, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta, StorageConfig.Value);
        var bytes = new byte[5];

        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(snapshotDataObjectName)).Throws(new FileNotFoundException());
        lowLevelStorageProvider.Setup(m => m.GetObjectContentLength(deltaDataObjectName)).ReturnsAsync(bytes.Length);

        Func<Task> snapshotStoreAction = () => persistentStorageProvider.StoreSnapshot(1, bytes, new MemoryStream(bytes));
        Func<Task> deltaStoreAction = () => persistentStorageProvider.StoreDelta(1, bytes, bytes);

        await snapshotStoreAction.Should().ThrowAsync<ExternalException>();
        await deltaStoreAction.Should().NotThrowAsync();

        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(snapshotDataObjectName), Times.Once);
        lowLevelStorageProvider.Verify(m => m.GetObjectContentLength(deltaDataObjectName), Times.Never);
    }
}
