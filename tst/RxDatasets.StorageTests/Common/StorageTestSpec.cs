using System.Diagnostics;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageTests.Common;

public abstract class StorageTestSpec
{
    protected IOptions<StorageConfig> StorageConfig = Options.Create(new StorageConfig());
    protected IOptions<SnapshotCreationConfig> SnapshotsOptions = Options.Create(new SnapshotCreationConfig() { SnapshotInterval = 10 });
    protected const ulong UNIT_KB = 1024;
    protected abstract Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(CancellationToken ct,
        [CallerMemberName] string memberName = "");

    [Fact]
    public async Task Should_Put_New_DTO()
    {
        var (reader, writer) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData();
        var initialVersion = reader.GetLatestVersion();
        var result = await writer.Put("Should_Put_New_DTO", randomBytes);

        result.Should().NotBeNull();
        result.NumAddedEntities.Should().Be(1);
        result.NewDatasetVersion.Should().Be(initialVersion + 1);
    }

    protected byte[] GenerateRandomData(ulong length = UNIT_KB, int seed = 0)
    {
        var randomData = new byte[length];
        var random = new Random(seed);

        random.NextBytes(randomData);
        return randomData;
    }


    [Fact]
    public async Task Should_Update_Changed_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB, 0);
        var randomBytes2 = GenerateRandomData(UNIT_KB, 1);
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var res = await azBlobStorageDatasetWriter.Put("Should_Not_Update_Same_DTO", randomBytes);
        var res2 = await azBlobStorageDatasetWriter.Put("Should_Not_Update_Same_DTO", randomBytes2);

        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);
        res.NumAddedEntities.Should().Be(1);
        res.NumUpdatedEntities.Should().Be(0);
        res.NumDeletedEntities.Should().Be(0);

        res2.NewDatasetVersion.Should().Be(res.NewDatasetVersion + 1);
        res2.NumAddedEntities.Should().Be(0);
        res2.NumUpdatedEntities.Should().Be(1);
        res2.NumDeletedEntities.Should().Be(0);
    }

    [Fact]
    public async Task Should_Not_Update_Unchanged_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var res = await azBlobStorageDatasetWriter.Put("Should_Not_Update_Same_DTO", randomBytes);
        var res2 = await azBlobStorageDatasetWriter.Put("Should_Not_Update_Same_DTO", randomBytes);

        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);
        res.NumAddedEntities.Should().Be(1);
        res.NumUpdatedEntities.Should().Be(0);
        res.NumDeletedEntities.Should().Be(0);

        res2.NewDatasetVersion.Should().Be(res.NewDatasetVersion);
        res2.NumAddedEntities.Should().Be(0);
        res2.NumUpdatedEntities.Should().Be(0);
        res2.NumDeletedEntities.Should().Be(0);
    }

    [Fact]
    public async Task Should_Put_All_New_DTOs_FluentAssertions()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>();
        for (int i = 0; i < 500; i++)
        {
            changes.Add(($"Should_Put_All_New_DTOs_{i}", GenerateRandomData(UNIT_KB, i), null, null));
        }

        var res = await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());

        res.Should().NotBeNull();
        res.NumAddedEntities.Should().Be(500);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);


        var readRes = (await azBlobStorageDatasetReader.BatchGet(changes.Select(c => c.name).ToList(), null)).ToList();
        for (var i = 0; i < changes.Count; i++)
        {
            readRes[i].name.Should().Be(changes[i].name);
        }
    }

    [Fact]
    public async Task Should_Put_All_Children_New_DTOs_FluentAssertions()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>();
        for (int i = 1; i <= 250; i++)
        {
            changes.Add(($"ParentEntity/A/Should_Put_All_Children_DTOs/{i}", GenerateRandomData(UNIT_KB, i), null, null));
        }

        for (int i = 251; i <= 500; i++)
        {
            changes.Add(($"ParentEntity/B/Should_Put_All_Children_DTOs/{i}", GenerateRandomData(UNIT_KB, i), null, null));
        }

        var res = await storageDatasetWriter.PutAllChildren(changes.ToAsyncEnumerable());

        res.Should().NotBeNull();
        res.NumAddedEntities.Should().Be(500);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);

        var readRes = (await azBlobStorageDatasetReader.BatchGet(changes.Select(c => c.name).ToList(), null)).ToList();
        for (var i = 0; i < changes.Count; i++)
        {
            readRes[i].name.Should().Be(changes[i].name);
        }

        var newChanges = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            ("ParentEntity/A/Should_Put_All_Children_DTOs/1", GenerateRandomData(UNIT_KB, 2), null, null), //to update
            ("ParentEntity/A/Should_Put_All_Children_DTOs/3", GenerateRandomData(UNIT_KB, 4), null, null), //to update
            ("ParentEntity/B/Should_Put_All_Children_DTOs/251", GenerateRandomData(UNIT_KB, 252), null, null), //to update
            ("ParentEntity/B/Should_Put_All_Children_DTOs/262", GenerateRandomData(UNIT_KB, 263), null, null), //to update
            ("ParentEntity/B/Should_Put_All_Children_DTOs/1000", GenerateRandomData(UNIT_KB, 1000), null, null), //to add
            ("ParentEntity/B/Should_Put_All_Children_DTOs/10001", GenerateRandomData(UNIT_KB, 1001), null, null) //to add
        };

        var newRes = await storageDatasetWriter.PutAllChildren(newChanges.ToAsyncEnumerable());

        newRes.Should().NotBeNull();
        newRes.NumAddedEntities.Should().Be(2);
        newRes.NumUpdatedEntities.Should().Be(4);
        newRes.NumDeletedEntities.Should().Be(496);
        newRes.NewDatasetVersion.Should().Be(initialDatasetVersion + 2);

    }

    [Fact(Skip = "Performance test that take around 2 min to load data into storage")]
    public async Task Should_Put_All_Children_DTOs_ShouldBeFast()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>();
        for (var channel = 100000; channel <= 100250; channel++)
        {
            for (var day = 20240101; day <= 20240124; day++)
            {
                for (var eventId = 1000000000; eventId <= 1000000024; eventId++)
                {
                    changes.Add(($"ParentEntity/{channel}/Should_Put_All_Children_DTOs/{day}/event/{eventId}", GenerateRandomData(UNIT_KB * 10), null, null));
                }
            }
        }

        var res = await storageDatasetWriter.PutAllChildren(changes.ToAsyncEnumerable());

        res.Should().NotBeNull();

        var newChanges = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>();
        for (var eventId = 1000000000; eventId <= 1000000024; eventId++)
        {
            newChanges.Add(($"ParentEntity/100077/Should_Put_All_Children_DTOs/20240122/event/{eventId}", GenerateRandomData(UNIT_KB * 10, 1), null, null));
            newChanges.Add(($"ParentEntity/100077/Should_Put_All_Children_DTOs/20240105/event/{eventId}", GenerateRandomData(UNIT_KB * 10, 1), null, null));
            newChanges.Add(($"ParentEntity/100075/Should_Put_All_Children_DTOs/20240121/event/{eventId}", GenerateRandomData(UNIT_KB * 10, 1), null, null));
            newChanges.Add(($"ParentEntity/100078/Should_Put_All_Children_DTOs/20240112/event/{eventId}", GenerateRandomData(UNIT_KB * 10, 1), null, null));
        }

        var sw = Stopwatch.StartNew();
        var newRes = await storageDatasetWriter.PutAllChildren(newChanges.ToAsyncEnumerable());
        sw.Stop();

        sw.ElapsedMilliseconds.Should().BeCloseTo(200, 50);
    }

    [Fact]
    public async Task Should_Put_Many_New_DTOs_FluentAssertions()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>();
        for (int i = 1; i <= 250; i++)
        {
            changes.Add(($"ParentEntity/A/Should_Put_All_Children_DTOs/{i}", GenerateRandomData(UNIT_KB, i), null, null));
        }

        for (int i = 251; i <= 500; i++)
        {
            changes.Add(($"ParentEntity/B/Should_Put_All_Children_DTOs/{i}", GenerateRandomData(UNIT_KB, i), null, null));
        }

        var res = await storageDatasetWriter.PutMany(changes.ToAsyncEnumerable());

        res.Should().NotBeNull();
        res.NumAddedEntities.Should().Be(500);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);

        var readRes = (await azBlobStorageDatasetReader.BatchGet(changes.Select(c => c.name).ToList(), null)).ToList();
        for (var i = 0; i < changes.Count; i++)
        {
            readRes[i].name.Should().Be(changes[i].name);
        }

        var newChanges = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            ("ParentEntity/A/Should_Put_All_Children_DTOs/1", GenerateRandomData(UNIT_KB, 2), null, null), //to update
            ("ParentEntity/A/Should_Put_All_Children_DTOs/3", GenerateRandomData(UNIT_KB, 4), null, null), //to update

            ("ParentEntity/B/Should_Put_All_Children_DTOs/251", GenerateRandomData(UNIT_KB, 252), null, null), //to update
            ("ParentEntity/B/Should_Put_All_Children_DTOs/262", GenerateRandomData(UNIT_KB, 263), null, null), //to update

            ("ParentEntity/B/Should_Put_All_Children_DTOs/1000", GenerateRandomData(UNIT_KB, 1000), null, null), //to add
            ("ParentEntity/B/Should_Put_All_Children_DTOs/10001", GenerateRandomData(UNIT_KB, 1001), null, null) //to add
        };

        var newRes = await storageDatasetWriter.PutMany(newChanges.ToAsyncEnumerable());

        newRes.Should().NotBeNull();
        newRes.NumAddedEntities.Should().Be(2);
        newRes.NumUpdatedEntities.Should().Be(4);
        newRes.NumDeletedEntities.Should().Be(0);
        newRes.NewDatasetVersion.Should().Be(initialDatasetVersion + 2);

    }


    [Fact]
    public async Task Should_Delete_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var key = "Should_Delete_DTO";

        await azBlobStorageDatasetWriter.Put("Should_Delete_DTO", randomBytes);
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var res = await azBlobStorageDatasetWriter.Delete(key);

        res.Should().NotBeNull();
        res.NumDeletedEntities.Should().Be(1);
        res.NumAddedEntities.Should().Be(0);
        res.NumUpdatedEntities.Should().Be(0);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);
    }

    [Fact]
    public async Task Should_Not_Delete_Deleted_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var key = "Should_Not_Delete_Deleted_DTO";

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var deleteResult = await azBlobStorageDatasetWriter.Delete(key);
        var secondDeleteResult = await azBlobStorageDatasetWriter.Delete(key);

        //Use fluent assertions for these assertions        //Use fluent assertions for these assertions
        deleteResult.Should().NotBeNull();
        deleteResult.NumDeletedEntities.Should().Be(1);
        deleteResult.NumAddedEntities.Should().Be(0);
        deleteResult.NumUpdatedEntities.Should().Be(0);
        deleteResult.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);

        secondDeleteResult.Should().NotBeNull();
        secondDeleteResult.NumDeletedEntities.Should().Be(0);
        secondDeleteResult.NumAddedEntities.Should().Be(0);
        secondDeleteResult.NumUpdatedEntities.Should().Be(0);
        secondDeleteResult.NewDatasetVersion.Should().Be(deleteResult.NewDatasetVersion);
    }

    [Fact]
    public async Task Should_Not_Delete_Non_Existent_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var key = "Should_Not_Delete_Non_Existent_DTO";
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var deleteResult = await azBlobStorageDatasetWriter.Delete(key);

        deleteResult.Should().NotBeNull();
        deleteResult.NumDeletedEntities.Should().Be(0);
        deleteResult.NumAddedEntities.Should().Be(0);
        deleteResult.NumUpdatedEntities.Should().Be(0);
        deleteResult.NewDatasetVersion.Should().Be(initialDatasetVersion);
    }

    [Fact]
    public async Task Should_DeleteChildren_DTOs_With_Valid_Ancestor()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var key = "providers/90/";

        await azBlobStorageDatasetWriter.Put("providers/90/asset/122", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/90/asset/1232", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/91/asset/12321", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/91/asset/12322", randomBytes);

        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var res = await azBlobStorageDatasetWriter.DeleteChildren(key);

        res.Should().NotBeNull();
        res.NumDeletedEntities.Should().Be(2);
        res.NumAddedEntities.Should().Be(0);
        res.NumUpdatedEntities.Should().Be(0);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);
    }

    [Fact]
    public async Task Should_Not_DeleteChildren_DTOs_With_Invalid_Ancestor()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var key = "parent";

        var res = async () => await azBlobStorageDatasetWriter.DeleteChildren(key);

        await res.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task Should_Not_DeleteChildren_Deleted_DTOs()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var randomBytes = GenerateRandomData(UNIT_KB);
        var key = "providers/90/";

        await azBlobStorageDatasetWriter.Put("providers/90/asset/122", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/90/asset/1232", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/91/asset/12321", randomBytes);
        await azBlobStorageDatasetWriter.Put("providers/91/asset/12322", randomBytes);
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var deleteResult = await azBlobStorageDatasetWriter.DeleteChildren(key);
        var secondDeleteResult = await azBlobStorageDatasetWriter.DeleteChildren(key);

        //Use fluent assertions for these assertions        //Use fluent assertions for these assertions
        deleteResult.Should().NotBeNull();
        deleteResult.NumDeletedEntities.Should().Be(2);
        deleteResult.NumAddedEntities.Should().Be(0);
        deleteResult.NumUpdatedEntities.Should().Be(0);
        deleteResult.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);

        secondDeleteResult.Should().NotBeNull();
        secondDeleteResult.NumDeletedEntities.Should().Be(0);
        secondDeleteResult.NumAddedEntities.Should().Be(0);
        secondDeleteResult.NumUpdatedEntities.Should().Be(0);
        secondDeleteResult.NewDatasetVersion.Should().Be(deleteResult.NewDatasetVersion);
    }

    [Fact]
    public async Task Should_Not_DeleteChildren_Non_Existent_DTOs()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var ancestor = "providers/90/";
        var initialDatasetVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var deleteResult = await azBlobStorageDatasetWriter.DeleteChildren(ancestor);

        deleteResult.Should().NotBeNull();
        deleteResult.NumDeletedEntities.Should().Be(0);
        deleteResult.NumAddedEntities.Should().Be(0);
        deleteResult.NumUpdatedEntities.Should().Be(0);
        deleteResult.NewDatasetVersion.Should().Be(initialDatasetVersion);
    }
    [Fact]
    public async Task Should_Read_After_Snapshot_Create()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var datasetVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var snapshotInterval = SnapshotsOptions.Value.SnapshotInterval;

        var data = GenerateRandomData(UNIT_KB);
        await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", GenerateRandomData(UNIT_KB, (int)datasetVersion));
        datasetVersion++;

        //Add new entries until the next snapshot creation
        while (datasetVersion % snapshotInterval > 0)
        {
            await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", GenerateRandomData(UNIT_KB, (int)datasetVersion));
            datasetVersion++;
        }

        await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", GenerateRandomData(UNIT_KB, (int)datasetVersion));
        datasetVersion++;

        for (int i = 0; i < (int)datasetVersion; i++)
        {
            var bytes = await azBlobStorageDatasetReader.Get($"Should_Initialize_Index_With_SnapShot_{i}");
            bytes.Should().NotBeNull();
            bytes.Value.name.Should().Be($"Should_Initialize_Index_With_SnapShot_{i}");
            bytes.Value.entityBytes.ToArray().Should().BeEquivalentTo(GenerateRandomData(UNIT_KB, i));
        }
    }


    [Fact]
    public async Task Should_Get_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var key = "Should_Get_Entity";
        var randomBytes = GenerateRandomData(UNIT_KB);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().NotBeNull();
        entity.Value.name.Should().Be(key);
        entity.Value.entityBytes.ToArray().Should().BeEquivalentTo(randomBytes);
    }

    [Fact]
    public async Task Should_Not_Get_Deleted_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var key = "Should_Not_Get_Deleted_Entity";
        var randomBytes = GenerateRandomData(UNIT_KB);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        await azBlobStorageDatasetWriter.Delete(key);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().BeNull();
    }

    [Fact]
    public async Task Should_Get_Undeleted_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var key = "Should_Get_Undeleted_Entity";
        var randomBytes = GenerateRandomData(UNIT_KB);
        var randomBytes2 = GenerateRandomData(UNIT_KB, 1);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        await azBlobStorageDatasetWriter.Delete(key);
        await azBlobStorageDatasetWriter.Put(key, randomBytes2);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().NotBeNull();
        entity!.Value.name.Should().Be(key);
        entity.Value.entityBytes.ToArray().Should().BeEquivalentTo(randomBytes2);
        entity.Value.entityBytes.ToArray().Should().NotBeEquivalentTo(randomBytes);
    }

    [Fact]
    public async Task Should_Not_Get_Non_Existing_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var key = "Should_Not_Get_Non_Existing_Entity";

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().BeNull();
    }


    [Fact]
    public async Task Should_Batch_Get()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        List<(string key, byte[]? bytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities = [];

        string parent = "testparent";
        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"{parent}/Should_Batch_Get_With_Parent_{i}", GenerateRandomData(UNIT_KB), null, null));
        }
        await storageDatasetWriter.PutAll(entities.ToAsyncEnumerable());

        var batchResults = (await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), parent)
            ).ToList();

        batchResults.Should().NotBeNull();
        batchResults.Should().HaveCount(entities.Count);

        foreach (var entity in entities)
        {
            var batchEntity = batchResults.First(x => x!.name == entity.key);

            batchEntity.Should().NotBeNull();
            batchEntity!.entityBytes?.ToArray().Should().BeEquivalentTo(entity.bytes);
        }
    }

    [Fact]
    public async Task Should_Not_Batch_Get_Deleted_Entity()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        List<(string key, byte[]? bytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities = [];

        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Batch_Get_Deleted_Entity_{i}", GenerateRandomData(UNIT_KB), null, null));
        }
        await storageDatasetWriter.PutAll(entities.ToAsyncEnumerable());

        var deletedEntities = new List<string>()
        {
            "Should_Not_Batch_Get_Deleted_Entity_3",
            "Should_Not_Batch_Get_Deleted_Entity_6"
        };

        foreach (var entityToDelete in deletedEntities)
        {
            await storageDatasetWriter.Delete(entityToDelete);
        }

        var result = (await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), null)).ToList();

        result.Should().NotBeNull();
        result.Should().HaveCount(entities.Count);
        result.Where(x => !x.entityBytes.HasValue).Should().HaveCount(deletedEntities.Count);

        foreach (var deletedEntity in deletedEntities)
        {
            result.Should().Contain(x => x.name == deletedEntity && x.entityBytes == null);
        }

        foreach (var entity in entities.Where(x => !deletedEntities.Contains(x.key)))
        {
            var resultEntity = result.Where(x => x.name == entity.key);

            resultEntity.Should().ContainSingle();
            resultEntity.First()!.entityBytes?.ToArray().Should().BeEquivalentTo(entity.bytes);
        }
    }

    [Fact]
    public async Task Should_Not_Batch_Get_Non_Existent_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        List<(string key, byte[]? bytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities = [];

        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Batch_Get_Deleted_Entity_{i}", GenerateRandomData(UNIT_KB), null, null));
        }
        await azBlobStorageDatasetWriter.PutAll(entities.ToAsyncEnumerable());

        var nonExistentEntities = new List<string>()
        {
            "Should_Not_Batch_Get_Deleted_Entity_11",
            "Should_Not_Batch_Get_Deleted_Entity_12"
        };

        var result = (await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key).Concat(nonExistentEntities), null)).ToList();

        result.Should().NotBeNull();
        result.Should().HaveCount(entities.Count + nonExistentEntities.Count);
        result.Where(x => x.entityBytes == null).Should().HaveCount(nonExistentEntities.Count);

        foreach (var deletedEntity in nonExistentEntities)
        {
            result.Should().Contain(x => x.name == deletedEntity && x.entityBytes == null);
        }

        foreach (var entity in entities)
        {
            var resultEntity = result.Where(x => x.name == entity.key);

            resultEntity.Should().ContainSingle();
            resultEntity.First()!.entityBytes?.ToArray().Should().BeEquivalentTo(entity.bytes);
        }
    }

    [Fact]
    public async Task Should_List_With_Parent_FluentAssertions()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Put entities with the same parent
        var bytes1 = GenerateRandomData(UNIT_KB, 1);
        await azBlobStorageDatasetWriter.Put("Parent1/Should_List_With_Parent_1", bytes1);

        var bytes2 = GenerateRandomData(UNIT_KB, 2);
        await azBlobStorageDatasetWriter.Put("Parent1/Should_List_With_Parent_2", bytes2);

        // Put entities with a different parent
        var bytes3 = GenerateRandomData(UNIT_KB, 3);
        await azBlobStorageDatasetWriter.Put("Parent2/Should_List_With_Parent_3", bytes3);

        // List entities with the matching parent
        var resultList = await azBlobStorageDatasetReader.BatchGet((await azBlobStorageDatasetReader.List("Parent1", CancellationToken.None)).Names, null);

        resultList.Should().HaveCount(2);

        var entity1 = resultList.First(x => x.name == "Parent1/Should_List_With_Parent_1");
        var entity2 = resultList.First(x => x.name == "Parent1/Should_List_With_Parent_2");

        entity1.entityBytes?.ToArray().Should().BeEquivalentTo(bytes1);
        entity2.entityBytes?.ToArray().Should().BeEquivalentTo(bytes2);
    }

    [Fact]
    public async Task Should_Not_List_Without_Parent()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        await azBlobStorageDatasetWriter.Put("Parent1/Should_Not_List_Without_Parent_1", GenerateRandomData(UNIT_KB));
        await azBlobStorageDatasetWriter.Put("Parent2/Should_Not_List_Without_Parent_2", GenerateRandomData(UNIT_KB));

        var resultList = await azBlobStorageDatasetReader.BatchGet((await azBlobStorageDatasetReader.List("Parent3", CancellationToken.None)).Names, null);

        resultList.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Not_List_Deleted_Entities()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        string parent = "Parent";
        string key = $"{parent}/Should_Not_List_Deleted_Entities";
        await azBlobStorageDatasetWriter.Put(key, GenerateRandomData(UNIT_KB));

        await azBlobStorageDatasetWriter.Delete(key);
        var resultList = await azBlobStorageDatasetReader.List(parent, CancellationToken.None);
        resultList.Names.Should().BeEmpty();
    }



    [Fact]
    public async Task Should_Get_Changed_Names_Updates()
    {
        //Should list updates after insert, then update.
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var key1 = "Should_Get_Changed_Names_Updates_1";
        var key2 = "Should_Get_Changed_Names_Updates_2";

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 1));
        var changedNames01 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).Changes.ToList();

        await azBlobStorageDatasetWriter.Put(key2, GenerateRandomData(UNIT_KB, 2));
        var changedNames02 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).Changes.ToList();

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 3));
        var changedNames03 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).Changes.ToList();

        var changedNames13 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 1)).Changes.ToList();
        var changedNames23 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 2)).Changes.ToList();

        changedNames01.Should().HaveCount(1);
        changedNames01.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.New);

        changedNames02.Should().HaveCount(2);
        changedNames02.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.New);
        changedNames02.Find(x => x.Name == key2).ChangeType.Should().Be(ChangeType.New);

        changedNames03.Should().HaveCount(2);
        changedNames03.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.New);
        changedNames03.Find(x => x.Name == key2).ChangeType.Should().Be(ChangeType.New);

        changedNames13.Should().HaveCount(2);
        changedNames13.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.Changed);
        changedNames13.Find(x => x.Name == key2).ChangeType.Should().Be(ChangeType.New);

        changedNames23.Should().HaveCount(1);
        changedNames23.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.Changed);
    }

    [Fact]
    public async Task Should_Get_Changed_Names_Inserts()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();

        var key1 = "Should_Get_Changed_Names_Updates_1";
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 1));
        var changedNames01 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).Changes.ToList();

        await azBlobStorageDatasetWriter.Delete(key1);
        var changedNames02 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 1)).Changes.ToList();

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 2));
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 3));
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_KB, 4));
        var changedNames05 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 3)).Changes.ToList();

        await azBlobStorageDatasetWriter.Delete(key1);
        var changedNames06 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 3)).Changes.ToList();

        changedNames01.Should().HaveCount(1);
        changedNames01.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.New);

        changedNames02.Should().HaveCount(1);
        changedNames02.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.Deleted);

        changedNames05.Should().HaveCount(1);
        changedNames05.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.Changed);

        changedNames06.Should().HaveCount(1);
        changedNames06.Find(x => x.Name == key1).ChangeType.Should().Be(ChangeType.Deleted);
    }

    [Fact]
    public async Task Should_Update_Latest_Version_Number_On_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();
        await azBlobStorageDatasetWriter.Put("Should_Update_Latest_Version_Number_On_Put", GenerateRandomData(UNIT_KB));

        var updatedVersion = azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion + 1);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Ignored_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        var bytes = GenerateRandomData(UNIT_KB);

        await azBlobStorageDatasetWriter.Put("Should_Not_Update_Latest_Version_Number_On_Ignored_Put", bytes);
        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetWriter.Put("Should_Not_Update_Latest_Version_Number_On_Ignored_Put", bytes);
        var updatedVersion = azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion);
    }

    [Fact]
    public async Task Should_Update_Latest_Version_Number_On_Put_All()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            ("Should_Update_Latest_Version_Number_On_Put_All_1", GenerateRandomData(UNIT_KB), null, null),
            ("Should_Update_Latest_Version_Number_On_Put_All_2", GenerateRandomData(UNIT_KB), null, null),
        };

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var updatedVersion = azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion + 1);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var changes = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            ("Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All_1", GenerateRandomData(UNIT_KB), null, null),
            ("Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All_2", GenerateRandomData(UNIT_KB), null, null),
        };

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var updatedVersion = azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Read()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        List<(string key, byte[]? bytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities = [];
        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Update_Latest_Version_Number_On_Read_{i}", GenerateRandomData(UNIT_KB), null, null));
        }
        await azBlobStorageDatasetWriter.PutAll(entities.ToAsyncEnumerable());
        var initialVersion = azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetReader.Get("Should_Not_Update_Latest_Version_Number_On_Read_Non_Existing");
        (azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.Get("Should_Not_Update_Latest_Version_Number_On_Read_1");
        (azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.GetChangedNames(0);
        (azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        (await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), null)).ToList();
        (azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.List("parent", CancellationToken.None);
        (azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);
    }



    [Fact]
    public async Task Should_Return_One_New_Entity_After_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        var bytes = GenerateRandomData(UNIT_KB);
        var putResult = await azBlobStorageDatasetWriter.Put("AddingOneShouldLeadToListOfOneNewEntity", bytes);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).Changes.ToList();


        // Then
        result.Where(x => x.ChangeType == ChangeType.New).Should().HaveCount(1);
        result.Where(x => x.ChangeType == ChangeType.New).Should().Contain(("AddingOneShouldLeadToListOfOneNewEntity", ChangeType.New));

        result.Where(x => x.ChangeType == ChangeType.Changed).Should().BeEmpty();
        result.Where(x => x.ChangeType == ChangeType.Deleted).Should().BeEmpty();
    }


    [Fact]
    public async Task Should_Return_One_New_Entity_After_Put_And_After_Waiting()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        CancellationToken cancellationToken = new();

        // Given
        var bytes = GenerateRandomData(UNIT_KB);

        Func<Task<ChangeSet>> action = async () => await azBlobStorageDatasetReader.WaitChangedNames(0, cancellationToken);
        var waitChangedListTask = action.Invoke();

        while (waitChangedListTask.Status != TaskStatus.WaitingForActivation)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
        waitChangedListTask.Status.Should().Be(TaskStatus.WaitingForActivation);

        var putResult = await azBlobStorageDatasetWriter.Put("AddingOneShouldLeadToListOfOneNewEntity", bytes, ct: cancellationToken);

        await waitChangedListTask.WaitAsync(TimeSpan.FromMilliseconds(1000), cancellationToken);

        // When
        var result = (await waitChangedListTask).Changes.ToList();


        // Then
        result.Where(x => x.ChangeType == ChangeType.New).Should().HaveCount(1);
        result.Where(x => x.ChangeType == ChangeType.New).Should().Contain(("AddingOneShouldLeadToListOfOneNewEntity", ChangeType.New));

        result.Where(x => x.ChangeType == ChangeType.Changed).Should().BeEmpty();
        result.Where(x => x.ChangeType == ChangeType.Deleted).Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Return_No_New_Entity_After_Canceling()
    {
        var (datasetReader, _) = await SetupStorageAsync(CancellationToken.None);
        CancellationTokenSource cancellationTokenSource = new();

        // Given
        var bytes = GenerateRandomData(UNIT_KB);

        Func<Task<ChangeSet>> action = async () => await datasetReader.WaitChangedNames(0, cancellationTokenSource.Token);
        var waitChangedListTask = action.Invoke();

        await Task.Delay(TimeSpan.FromSeconds(0.3), cancellationTokenSource.Token);

        waitChangedListTask.Status.Should().Be(TaskStatus.WaitingForActivation);
        await cancellationTokenSource.CancelAsync();

        await Task.Delay(TimeSpan.FromSeconds(0.2)); //wait to return from listTask

        waitChangedListTask.Status.Should().Be(TaskStatus.Canceled);
    }

    [Fact]
    public async Task Should_Return_Empty_Changelist_On_Delete_After_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToEmptyResultOverBothChanges";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));
        var deleteResult = await azBlobStorageDatasetWriter.Delete(name);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).Changes.ToList();

        // Then
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Return_Single_New_In_Changelist_On_Update_After_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToEmptyResultOverBothChanges";
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_Changed_In_Changelist_On_Update_After_Insert_From_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "AddingOneThenChangingOneShouldLeadToListWithChangeOverTheLastDeltaOnly";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));
        var updateResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(putResult.NewDatasetVersion)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_Deleted_In_Changelist_On_Delete_After_Update()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToListWithDeleteOverTheLastDeltaOnly";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));
        var deleteResult = await azBlobStorageDatasetWriter.Delete(name);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(putResult.NewDatasetVersion)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Deleted));
    }

    [Fact]
    public async Task Should_Return_Nothing_In_Changelist_On_FID()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Nothing_In_Changelist_On_FID";

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDID()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDID";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Deleted));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDI()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDI";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_FIDI()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_FIDI";

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFC";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_FIC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_FIC";

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDIC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDIC";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //F
        var from = azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB));

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_KB, 1));

        //T
        var to = azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).Changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Mix_Puts_And_Deletes_In_PutOrDeleteMany()
    {
        var (reader, writer) = await SetupStorageAsync(CancellationToken.None);
        // Seed some entities
        await writer.Put("Mix/1", GenerateRandomData(UNIT_KB, 1));
        await writer.Put("Mix/2", GenerateRandomData(UNIT_KB, 2));
        await writer.Put("Mix/3", GenerateRandomData(UNIT_KB, 3));
        var initialVersion = reader.GetLatestVersion();

        var randomNew = GenerateRandomData(UNIT_KB, 10);
        var randomUpdate = GenerateRandomData(UNIT_KB, 20);

        var mixed = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            ("Mix/2", randomUpdate, null, null), // update existing
            ("Mix/3", null, null, null), // delete existing
            ("Mix/4", randomNew, null, null), // new entity
            ("Mix/DoesNotExist", null, null, null) // delete non-existing ignored
        };

        var res = await writer.PutOrDeleteMany(mixed.ToAsyncEnumerable());

        res.NewDatasetVersion.Should().Be(initialVersion + 1);
        res.NumAddedEntities.Should().Be(1);
        res.NumUpdatedEntities.Should().Be(1);
        res.NumDeletedEntities.Should().Be(1);

        // Validate state
        (await reader.Get("Mix/2")).Value.entityBytes.ToArray().Should().BeEquivalentTo(randomUpdate);
        (await reader.Get("Mix/3")).Should().BeNull();
        (await reader.Get("Mix/4")).Value.entityBytes.ToArray().Should().BeEquivalentTo(randomNew);
    }




}


