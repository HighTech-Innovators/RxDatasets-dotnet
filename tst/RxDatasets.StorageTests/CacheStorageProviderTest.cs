using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageTests
{
    public class CacheStorageProviderTest
    {
        private const ulong UNIT_KB = 1024 * 1024;

        private (IStorageProvider<object> cacheStorageProvider, Mock<IStorageProvider<object>> storageProviderMock) CreateCacheStorageProvider(int cacheDurationSec)
        {
            var storageLogger = new Mock<ILogger<CacheStorageProvider<object>>>();
            var cacheConfig = Options.Create(new DtoStorageCacheConfig()
            {
                CacheSizeMB = 100
            });
            var storageProviderMock = new Mock<IStorageProvider<object>>();
            var notificationsHubMock = new Mock<INotificationsHub<object>>();
            var cacheStorageProvider = new CacheStorageProvider<object>(storageProviderMock.Object, cacheConfig, storageLogger.Object, notificationsHubMock.Object);

            return (cacheStorageProvider, storageProviderMock);
        }

        private byte[] GenerateRandomData(ulong length = UNIT_KB, int seed = 0)
        {
            var randomData = new byte[length];
            var random = new Random(seed);
            random.NextBytes(randomData);

            return randomData;
        }

        [Fact]
        public async Task Should_Hit_Cache_On_Partial_File_Download()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(10);

            var dataBytes = GenerateRandomData();
            var indexBytes = GenerateRandomData();

            ulong version = 1;

            await cacheStorageProvider.StoreDelta(version, indexBytes, dataBytes);
            var returnedBytes = await cacheStorageProvider.ReadDataFromSources([(ObjectNameTypeEnum.DELTA, version, 256)], 512);
            var expectedBytes = dataBytes.Skip(256).Take(512).ToArray();

            //Assert
            Assert.Equal(expectedBytes, returnedBytes.ToArray());
            storageProviderMock.Verify(x => x.ReadDataFromSources(
                It.IsAny<List<(ObjectNameTypeEnum type, ulong version, long position)>>(), 512), Times.Never);
            storageProviderMock.Verify(x => x.StoreDelta(version, It.IsAny<byte[]>(), It.IsAny<byte[]>()), Times.Once);
        }

        [Fact]
        public async Task Should_Not_Hit_Cache_For_Delta_Index()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(10);

            var dataBytes = GenerateRandomData();
            var indexBytes = GenerateRandomData();

            ulong version = 1;

            await cacheStorageProvider.StoreDelta(version, indexBytes, dataBytes);
            var returnedDeltaIndex = await cacheStorageProvider.ReadDeltaIndex(version);

            //Assert
            storageProviderMock.Verify(x => x.ReadDeltaIndex(It.IsAny<ulong>()), Times.Once);
            storageProviderMock.Verify(x => x.StoreDelta(version, It.IsAny<byte[]>(), It.IsAny<byte[]>()), Times.Once);
        }

        [Fact]
        public async Task Should_Not_Hit_Cache_For_Snapshot_Index()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(10);

            var dataBytes = GenerateRandomData();
            var indexBytes = GenerateRandomData();

            ulong version = 1;

            await using var filestream = File.OpenWrite("test.pb");
            filestream.Write(dataBytes);
            filestream.Position = 0;
            filestream.Close();
            await using var readFileStream = File.OpenRead("test.pb");
            await cacheStorageProvider.StoreSnapshot(version, indexBytes, readFileStream);
            readFileStream.Close();

            var returnedSnapshotIndex = await cacheStorageProvider.ReadSnapshotIndex(version);

            //Assert
            storageProviderMock.Verify(x => x.ReadSnapshotIndex(It.IsAny<ulong>()), Times.Once);
            storageProviderMock.Verify(x => x.StoreSnapshot(version, It.IsAny<byte[]>(), It.IsAny<byte[]>()), Times.Once);
        }

        [Fact]
        public async Task Should_Hit_Cache_For_Partial_Read_Snapshot_Data()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(10);

            var dataBytes = GenerateRandomData();
            var indexBytes = GenerateRandomData();

            ulong version = 1;

            await using var filestream = File.OpenWrite("test.pb");
            filestream.Write(dataBytes);
            filestream.Position = 0;
            filestream.Close();
            await using var readFileStream = File.OpenRead("test.pb");
            await cacheStorageProvider.StoreSnapshot(version, indexBytes, readFileStream);
            readFileStream.Close();
            var returnedData = await cacheStorageProvider.ReadDataFromSources([(ObjectNameTypeEnum.SNAPSHOT, version, 256)], 512);

            var expectedBytes = dataBytes.Skip(256).Take(512).ToArray();

            //Assert
            storageProviderMock.Verify(x => x.ReadDataFromSources(
                It.IsAny<List<(ObjectNameTypeEnum type, ulong version, long position)>>(), 512), Times.Never);
            storageProviderMock.Verify(x => x.StoreSnapshot(version, It.IsAny<byte[]>(), It.IsAny<byte[]>()), Times.Once);
        }
        [Fact]
        public async Task ReadDataFromSources_Should_Throw_If_No_Data_Source_Is_Provided()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(10);

            var action = async () => await cacheStorageProvider.ReadDataFromSources([], 512);

            //Assert
            await action.Should().ThrowAsync<ArgumentException>();
        }
        [Fact]
        public async Task Should_Hit_Cache_On_Callback_Read()
        {
            var (cacheStorageProvider, storageProviderMock) = CreateCacheStorageProvider(1);

            var dataBytes = GenerateRandomData();
            var indexBytes = GenerateRandomData();

            ulong version = 1;

            await using var filestream = File.OpenWrite("test.pb");
            filestream.Write(dataBytes);
            filestream.Position = 0;
            filestream.Close();
            await using var readFileStream = File.OpenRead("test.pb");
            await cacheStorageProvider.StoreSnapshot(version, indexBytes, readFileStream);
            readFileStream.Close();

            await cacheStorageProvider.ReadSnapshotData(version, (stream) =>
            {
                var returnedBytes = new byte[stream.Length];
                stream.Read(returnedBytes, 0, returnedBytes.Length);
            });

            //Assert
            storageProviderMock.Verify(x => x.StoreSnapshot(version, It.IsAny<byte[]>(), It.IsAny<byte[]>()), Times.Once);
            storageProviderMock.Verify(x => x.ReadSnapshotData(It.IsAny<ulong>(), It.IsAny<Action<Stream>>()), Times.Never);
        }
    }
}
