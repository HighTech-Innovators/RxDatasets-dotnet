using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FluentAssertions;
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
    private readonly LowLevelInMemoryStorageProvider<object> _storageProvider;

    public UsingLowLevelInMemoryStorage()
    {
        var storageLogger = new Mock<ILogger<LowLevelInMemoryStorageProvider<object>>>();

        _storageProvider = new LowLevelInMemoryStorageProvider<object>(storageLogger.Object);
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        
        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, _storageProvider, notificationsHub, false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);
        IOptions<SnapshotCreationConfig> snapshotCreationConfig = Options.Create(new SnapshotCreationConfig {SnapshotInterval = 5});

        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, persistentStorageProvider, snapshotCreationConfig, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>> (datasetUpdateConsumingServiceLogger.Object, notificationsHub, snapshotCreationPlugin);
        await datasetUpdateConsumingService.StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(notificationsHub, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig()), false);
        return (datasetReader, datasetWriter);
    }

    [Fact]
    public async Task Should_Initialize_Index_With__SnapShot()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);
        
        // Act
        await SnapshotCreationPluginTests.CreateNumberDeltas(11, datasetWriter);
        
        // Assert
        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(11, "Create a new Dataset instance, they should return identical results");
        newReader.Dispose();
    }

    [Fact]
    public async Task Should_Initialize_Index_With_SnapShot_That_Only_Contains_Deleted_Entities()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Act
        await SnapshotCreationPluginTests.CreateNumberDeltas(10, datasetWriter);
        await datasetWriter.PutAll(AsyncEnumerable.Empty<(string, byte[])>());

        // Assert
        var snapshotObjectName = ObjectStorageProvider<object>.GetObjectName(11, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, new StorageConfig());
        var actualSnapshotLength = await _storageProvider.GetObjectContentLength(snapshotObjectName);
        actualSnapshotLength.Should().Be(1);

        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(11, "Create a new Dataset instance, they should return identical results");
        newReader.Dispose();
    }

    [Fact]
    public async Task Should_Initialize_Index_With_SnapShot_That_Only_Contains_Deleted_Entities_Even_If_The_Snapshot_Data_Is_Absent()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Act
        await SnapshotCreationPluginTests.CreateNumberDeltas(10, datasetWriter);
        await datasetWriter.PutAll(AsyncEnumerable.Empty<(string, byte[])>());
        await _storageProvider.DeleteFile(ObjectStorageProvider<object>.GetObjectName(11, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, new StorageConfig()));

        // Assert
        var (newReader, _) = await SetupStorageAsync(CancellationToken.None);
        newReader.GetLastUsedSnapshotVersion().Should().Be(11, "Create a new Dataset instance, they should return identical results");
        newReader.Dispose();
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Index_Does_Not_Exist()
    {
        // Arrange
        var (datasetReader, datasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await SnapshotCreationPluginTests.CreateNumberDeltas(11, datasetWriter);

        datasetReader.GetLastUsedSnapshotVersion().Should().Be(11, "Since we add 6 deltas, snapshot 1, 6, 11 must be created");
        datasetReader.Dispose();

        // Act
        await _storageProvider.DeleteFile(ObjectStorageProvider<object>.GetObjectName(11, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, new StorageConfig()));

        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(6, "Because we removed the last snapshot version 11 data");
        toTestReader.GetLatestVersion().Should().Be(11, "We should read all the deltas after the earlier snapshot");

        var changes = await toTestReader.GetChangedNames(0);
        changes.changes.Where(ch => ch.changeType == ChangeType.New).Should().HaveCount(11, "We should observe all 11 insertions, so there is no possibility of lying about versions based on assumptions from the published snapshot version");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Data_Does_Not_Exist()
    {
        // Arrange
        var (datasetReader, datasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await SnapshotCreationPluginTests.CreateNumberDeltas(11, datasetWriter);

        datasetReader.GetLastUsedSnapshotVersion().Should().Be(11, "Since we add 6 deltas, snapshot 1, 6, 11 must be created");
        datasetReader.Dispose();
        
        // Act
        await _storageProvider.DeleteFile(ObjectStorageProvider<object>.GetObjectName(11, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, new StorageConfig()));
        
        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(6, "Because we removed the last snapshot version 11 data");
        toTestReader.GetLatestVersion().Should().Be(11, "We should read all the deltas after the earlier snapshot");

        var changes = await toTestReader.GetChangedNames(0);
        changes.changes.Where(ch => ch.changeType == ChangeType.New).Should().HaveCount(11, "We should observe all 11 insertions, so there is no possibility of lying about versions based on assumptions from the published snapshot version");
    }

    [Fact]
    public async Task Should_Initialize_Index_With_Older_SnapShot_When_Last_SnapShot_Does_Exist_But_Is_Corrupted()
    {
        // Arrange
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await SnapshotCreationPluginTests.CreateNumberDeltas(11, datasetWriter);
        
        // Act
        var snapshotObjectName = ObjectStorageProvider<object>.GetObjectName(11, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, new StorageConfig());
        var actualSnapshotLength = await _storageProvider.GetObjectContentLength(snapshotObjectName);
        var wrongBytes = new byte[actualSnapshotLength - 1]; // This should fail since it short
        await _storageProvider.UploadBytes(snapshotObjectName, wrongBytes);
        
        // Assert
        var (toTestReader, _) = await SetupStorageAsync(CancellationToken.None);
        toTestReader.GetLastUsedSnapshotVersion().Should().Be(6, "Because we corrupted the last snapshot version 11");
    }
    
    private async Task<IStorageProvider<object>> SetupProviderAsync(UploadVerificationMode verificationMode, 
                                                                    Mock<IObjectStorageProvider<object>> lowLevelStorageProvider)
    {
        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        StorageConfig.Value.UploadVerificationMode = verificationMode;
        var persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, lowLevelStorageProvider.Object, notificationsHub, false);
        await persistentStorageProvider.StartAsync(CancellationToken.None);

        return persistentStorageProvider;
    }

    [Fact]
    public async Task Verify_Data_Existence_After_Upload_With_IncorrectData()
    {
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.DoNotVerify, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyAll, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyLargeDataOnly, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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
        var lowLevelStorageProvider = new Mock<IObjectStorageProvider<object>>();
        var persistentStorageProvider = await SetupProviderAsync(UploadVerificationMode.VerifyLargeDataOnly, lowLevelStorageProvider);

        var snapshotDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, StorageConfig.Value);
        var deltaDataObjectName = ObjectStorageProvider<object>.GetObjectName(1, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, StorageConfig.Value);
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

