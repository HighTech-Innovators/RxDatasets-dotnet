using System.Data;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageTests.Services;

/// <summary>
/// #79 item 4: on storage with compare-and-swap, a delta is committed by creating its data and then its index only if
/// absent, so a writer that is behind (here: a second writer that never sees the first one's deltas) fails instead of
/// overwriting a stored delta, and reloads.
/// </summary>
public sealed class FencedDeltaCommitTests : IDisposable
{
    private static readonly IOptions<StorageConfig> StorageConfig = Options.Create(new StorageConfig { DtoName = "Fenced" });

    private readonly string _rootFolder = Path.Combine(Path.GetTempPath(), $"rxdatasets-fenced-{Guid.NewGuid():N}");
    private readonly LocalDiskStorage<object> _storage;

    public FencedDeltaCommitTests()
    {
        _storage = new LocalDiskStorage<object>(NullLogger<LocalDiskStorage<object>>.Instance,
            Options.Create(new LocalDiskStorageConfig { RootFolder = _rootFolder }));
    }

    public void Dispose()
    {
        try { Directory.Delete(_rootFolder, true); } catch (IOException) { }
    }

    [Fact]
    public async Task Writer_That_Is_Behind_Cannot_Overwrite_A_Stored_Delta_And_Reloads()
    {
        var a = await StartWriter();
        var b = await StartWriter();
        await a.Put("a", [1], null, null);
        var storedData = await Read(1, ObjectNameIndexOrDataEnum.Data);
        var storedIndex = await Read(1, ObjectNameIndexOrDataEnum.Index);

        Func<Task> staleWrite = () => b.Put("b", [2], null, null);
        await staleWrite.Should().ThrowAsync<DBConcurrencyException>();

        (await Read(1, ObjectNameIndexOrDataEnum.Data)).Should().Equal(storedData);
        (await Read(1, ObjectNameIndexOrDataEnum.Index)).Should().Equal(storedIndex);

        // After its reload, the writer continues from the stored delta
        var response = await PutWhenReady(b, "b", [2]);
        response.NewDatasetVersion.Should().Be(2);
        (await b.Get("a")).Should().NotBeNull();
    }

    [Fact]
    public async Task Data_Left_By_A_Crashed_Writer_Is_Replaced_Once_It_Is_Old()
    {
        var writer = await StartWriter();
        await _storage.UploadBytes(Name(1, ObjectNameIndexOrDataEnum.Data), [9, 9, 9]);
        File.SetLastWriteTimeUtc(Path.Combine(_rootFolder, Name(1, ObjectNameIndexOrDataEnum.Data)),
            DateTime.UtcNow.AddMinutes(-5));

        var response = await writer.Put("a", [1], null, null);

        response.NewDatasetVersion.Should().Be(1);
        (await writer.Get("a"))!.Value.entityBytes.ToArray().Should().Equal([1]);
    }

    [Fact]
    public async Task Recent_Uncommitted_Data_Is_A_Conflict()
    {
        var writer = await StartWriter();
        await _storage.UploadBytes(Name(1, ObjectNameIndexOrDataEnum.Data), [9, 9, 9]);

        Func<Task> write = () => writer.Put("a", [1], null, null);

        await write.Should().ThrowAsync<DBConcurrencyException>();
        (await Read(1, ObjectNameIndexOrDataEnum.Data)).Should().Equal([9, 9, 9]);
    }

    [Fact]
    public async Task Writer_Behind_Cleaning_Does_Not_Recreate_A_Removed_Delta()
    {
        var a = await StartWriter();
        await a.Put("a", [1], null, null);
        var b = await StartWriter(); // at version 1
        await a.Put("a", [2], null, null);
        await a.Put("a", [3], null, null);
        // What cleaning without KeepAllDeltas leaves after a snapshot at 3: no deltas 1..3
        for (ulong version = 1; version <= 3; version++)
            await _storage.DeleteFolder(DatasetHelper.GetObjectFolder<object>(version, ObjectNameTypeEnum.Delta,
                StorageConfig.Value) + '/');
        await _storage.UploadBytes(DatasetHelper.GetObjectName<object>(3, ObjectNameIndexOrDataEnum.Index,
            ObjectNameTypeEnum.Snapshot, StorageConfig.Value), [0]);

        Func<Task> staleWrite = () => b.Put("b", [4], null, null);

        await staleWrite.Should().ThrowAsync<DBConcurrencyException>();
        (await _storage.ListObjects(DatasetHelper.GetObjectFolder<object>(2, ObjectNameTypeEnum.Delta,
            StorageConfig.Value) + '/')).Should().BeEmpty();
    }

    private async Task<DatasetReaderWriter<object>> StartWriter()
    {
        INotificationsHub<object> hub = new NotificationsHubSync<object>();
        var reader = new ObjectStorageReader<object>(NullLogger<ObjectStorageReader<object>>.Instance, StorageConfig, _storage);
        var provider = new ObjectStorageProvider<object>(NullLogger<ObjectStorageProvider<object>>.Instance, StorageConfig,
            _storage, hub, reader, false);
        var writer = new DatasetReaderWriter<object>(hub, NullLogger<DatasetWriter<object>>.Instance, provider,
            Options.Create(new WriterConfig()));
        await provider.StartAsync(CancellationToken.None);
        return writer;
    }

    private static async Task<IDatasetWriter<object>.WriteResponse> PutWhenReady(DatasetReaderWriter<object> writer,
        string name, byte[] value)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await writer.Put(name, value, null, null);
            }
            catch (Exception) when (attempt < 100)
            {
                await Task.Delay(50);
            }
        }
    }

    private static string Name(ulong version, ObjectNameIndexOrDataEnum part) =>
        DatasetHelper.GetObjectName<object>(version, part, ObjectNameTypeEnum.Delta, StorageConfig.Value);

    private async Task<byte[]> Read(ulong version, ObjectNameIndexOrDataEnum part) =>
        (await _storage.DownloadBytes(Name(version, part))).ToArray();
}
