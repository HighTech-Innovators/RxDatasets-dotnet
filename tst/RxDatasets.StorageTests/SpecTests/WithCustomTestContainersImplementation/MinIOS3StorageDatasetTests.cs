using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.Minio;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class MinIOS3StorageDatasetTests : StorageTestSpec, IClassFixture<MinioTestContainerFixture>, IAsyncLifetime
{
    private readonly MinioTestContainerFixture _minioContainer;
    private IOptions<MinioS3Config>? _s3Config;
    private IOptions<WriterConfig>? _writerConfig;

    public MinIOS3StorageDatasetTests(MinioTestContainerFixture minioContainer)
    {
        _minioContainer = minioContainer;
    }

    public async Task InitializeAsync()
    {
        if (_minioContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await _minioContainer.Container.StartAsync();
        _s3Config = Options.Create<MinioS3Config>(new()
        {
            AccessKey = _minioContainer.Container.GetAccessKey(),
            SecretKey = _minioContainer.Container.GetSecretKey(),
            ServiceURL = $"{_minioContainer.Container.Hostname}:{_minioContainer.Container.GetMappedPublicPort(9000)}"
        });
        _writerConfig = Options.Create<WriterConfig>(new() { AllowedNamesPattern = "" });
    }

    public async Task DisposeAsync()
    {
        await _minioContainer.DisposeAsync();
    }


    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        _s3Config!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        var storageLogger = new Mock<ILogger<MinioS3StorageProvider<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();


        IObjectStorageProvider<object> s3StorageProvider = new MinioS3StorageProvider<object>(storageLogger.Object, _s3Config!, false);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, DtoOptions, s3StorageProvider, notificationsHub);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(notificationsHub, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, _writerConfig, false);


        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_minioS3_After_Initialization()
    {
        _s3Config!.Value.BucketName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_minioS3_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, datasetWriter) = await SetupStorageAsync();
        await _minioContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => datasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_Storage_After_Initialization", bytes);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_minioS3()
    {
        var (_, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_S3_Storage", bytes);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }

    [Theory]
    [MemberData(nameof(TestData))]
    public async Task Should_Throw_ArgumentOutOfRangeException_On_Invalid_Name(
        string validationRegex, string name, bool shouldSucceed)
    {
        // Arrange
        _writerConfig!.Value.AllowedNamesPattern = validationRegex;

        // Assuming SetupStorageAsync sets up the necessary objects
        var (_, datasetWriter) = await SetupStorageAsync();
        var bytes = GenerateRandomData();

        // Act
        Func<Task> putAction = () => datasetWriter.Put(name, bytes);

        // Assert
        if (shouldSucceed)
        {
            await putAction.Should().NotThrowAsync();
        }
        else
        {
            await putAction.Should().ThrowAsync<ArgumentOutOfRangeException>();
        }
    }

    public static IEnumerable<object[]> TestData()
    {
        return
          [
            ["", "validName", true],               // Should pass: empty pattern
          ["^[a-zA-Z]+$", "ValidName", true],    // Should pass: only letters
          ["^[a-zA-Z]+$", "Invalid123", false],  // Should fail: contains numbers
          ["^\\d+$", "12345", true],             // Should pass: only digits
          ["^\\d+$", "abc123", false],           // Should fail: contains letters
          ["^[a-z]+$", "lowercase", true],       // Should pass: lowercase letters
          ["^[a-z]+$", "UPPERCASE", false],      // Should fail: uppercase letters
          ["^$", "", true],                      // Should pass: empty string matches empty pattern
          ["^$", "nonempty", false],             // Should fail: non-empty string with empty pattern
        ];
    }

}