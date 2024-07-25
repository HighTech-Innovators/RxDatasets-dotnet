using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageTests.Common;

public abstract class StorageTestSpec
{
    protected IOptions<StorageConfig> DtoOptions = Options.Create(new StorageConfig());
    protected IOptions<SnapshotCreationConfig> SnapshotsOptions = Options.Create(new SnapshotCreationConfig());
    private const ulong UNIT_KB = 1024;
    private const ulong UNIT_MB = 1024 * UNIT_KB;
    protected abstract Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "");
    
    [Fact]
    public async Task Should_Put_New_DTO()
    {
        var (reader, writer) = await SetupStorageAsync();
        var randomBytes = GenerateRandomData();
        var initialVersion = await reader.GetLatestVersion();
        var result = await writer.Put("Should_Put_New_DTO", randomBytes);

        result.Should().NotBeNull();
        result.NumAddedEntities.Should().Be(1);
        result.NewDatasetVersion.Should().Be(initialVersion + 1);
    }

    protected byte[] GenerateRandomData(ulong length = UNIT_MB, int seed = 0)
    {
        var randomData = new byte[length];
        var random = new Random(seed);

        random.NextBytes(randomData);
        return randomData;
    }


    [Fact]
    public async Task Should_Update_Changed_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var randomBytes = GenerateRandomData(UNIT_MB, 0);
        var randomBytes2 = GenerateRandomData(UNIT_MB, 1);
        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var randomBytes = GenerateRandomData(UNIT_MB);
        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string name, byte[])>();
        for (int i = 0; i < 500; i++)
        {
            changes.Add(($"Should_Put_All_New_DTOs_{i}", GenerateRandomData(UNIT_MB, i)));
        }

        var res = await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());

        res.Should().NotBeNull();
        res.NumAddedEntities.Should().Be(500);
        res.NewDatasetVersion.Should().Be(initialDatasetVersion + 1);


        var readRes = await azBlobStorageDatasetReader.BatchGet(changes.Select(c=>c.name).ToList(), null).ToListAsync();
        for (var i = 0; i < changes.Count; i++)
        {
            readRes[i].name.Should().Be(changes[i].name);
        }
    }

    [Fact]
    public async Task Should_Delete_DTO()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var randomBytes = GenerateRandomData(UNIT_MB);
        var key = "Should_Delete_DTO";

        await azBlobStorageDatasetWriter.Put("Should_Delete_DTO", randomBytes);
        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var randomBytes = GenerateRandomData(UNIT_MB);
        var key = "Should_Not_Delete_Deleted_DTO";

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var key = "Should_Not_Delete_Non_Existent_DTO";
        var initialDatasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        var deleteResult = await azBlobStorageDatasetWriter.Delete(key);

        deleteResult.Should().NotBeNull();
        deleteResult.NumDeletedEntities.Should().Be(0);
        deleteResult.NumAddedEntities.Should().Be(0);
        deleteResult.NumUpdatedEntities.Should().Be(0);
        deleteResult.NewDatasetVersion.Should().Be(initialDatasetVersion);
    }

    [Fact]
    public async Task Should_Read_After_Snapshot_Create()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var datasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();
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
    public async Task Should_Initialize_Index_With_SnapShot()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var datasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();
        var snapshotInterval = SnapshotsOptions.Value.SnapshotInterval;

        var data = GenerateRandomData(UNIT_KB);
        await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);
        datasetVersion++;

        //Add new entries until the next snapshot creation
        while (datasetVersion % snapshotInterval > 0)
        {
            await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);
            datasetVersion++;
        }

        //Create a new Dataset instance, they should return identical results
        var (newReader, newWriter) = await SetupStorageAsync();
        var datasetVersionOldDataset = await azBlobStorageDatasetReader.GetLatestVersion();
        var datasetVersionNewDataset = await newReader.GetLatestVersion();

        datasetVersionOldDataset.Should().Be(datasetVersionNewDataset);

        for (ulong i = 0; i < datasetVersion; i++)
        {
            var bytesOldDataset = await azBlobStorageDatasetReader.Get($"Should_Initialize_Index_With_SnapShot_{i}");
            var bytesNewDataset = await newReader.Get($"Should_Initialize_Index_With_SnapShot_{i}");

            bytesOldDataset.Should().NotBeNull();
            bytesNewDataset.Should().NotBeNull();

            bytesOldDataset.Value.name.Should().BeEquivalentTo(bytesNewDataset.Value.name);
            bytesNewDataset.Value.entityBytes.ToArray().Should().BeEquivalentTo(bytesOldDataset.Value.entityBytes.ToArray());
        }
    }

    [Fact]
    public async Task Should_Initialize_Index_Delta()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var datasetVersion = await azBlobStorageDatasetReader.GetLatestVersion();
        var snapshotInterval = SnapshotsOptions.Value.SnapshotInterval;

        var data = GenerateRandomData(UNIT_KB);

        //Add new entries until the next snapshot creation
        while (datasetVersion % snapshotInterval > 0)
        {
            datasetVersion++;
            await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);
        }

        //Now do 1 more, so we for sure have 1 put taking place after the snapshot
        datasetVersion++;
        await azBlobStorageDatasetWriter.Put($"Should_Initialize_Index_With_SnapShot_{datasetVersion}", data);

        //Create a new Dataset instance, they should return identical results
        var (newReader, newWriter) = await SetupStorageAsync();
        var datasetVersionNewDataset = await newReader.GetLatestVersion();

        datasetVersionNewDataset.Should().Be(datasetVersion);

        var bytesOldDataset = await azBlobStorageDatasetReader.Get($"Should_Initialize_Index_With_SnapShot_{datasetVersion}");
        var bytesNewDataset = await newReader.Get($"Should_Initialize_Index_With_SnapShot_{datasetVersion}");

        bytesNewDataset.Should().NotBeNull();
        bytesNewDataset.Value.entityBytes.ToArray().Should().BeEquivalentTo(bytesOldDataset.Value.entityBytes.ToArray());
    }

    [Fact]
    public async Task Should_Get_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var key = "Should_Get_Entity";
        var randomBytes = GenerateRandomData(UNIT_MB);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().NotBeNull();
        entity.Value.name.Should().Be(key);
        entity.Value.entityBytes.ToArray().Should().BeEquivalentTo(randomBytes);
    }

    [Fact]
    public async Task Should_Not_Get_Deleted_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var key = "Should_Not_Get_Deleted_Entity";
        var randomBytes = GenerateRandomData(UNIT_MB);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        await azBlobStorageDatasetWriter.Delete(key);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().BeNull();
    }

    [Fact]
    public async Task Should_Get_Undeleted_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var key = "Should_Get_Undeleted_Entity";
        var randomBytes = GenerateRandomData(UNIT_MB);
        var randomBytes2 = GenerateRandomData(UNIT_MB, 1);

        await azBlobStorageDatasetWriter.Put(key, randomBytes);
        await azBlobStorageDatasetWriter.Delete(key);
        await azBlobStorageDatasetWriter.Put(key, randomBytes2);

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().NotBeNull();
        entity.Value.name.Should().Be(key);
        entity.Value.entityBytes.ToArray().Should().BeEquivalentTo(randomBytes2);
        entity.Value.entityBytes.ToArray().Should().NotBeEquivalentTo(randomBytes);
    }

    [Fact]
    public async Task Should_Not_Get_Non_Existing_Entity()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var key = "Should_Not_Get_Non_Existing_Entity";

        var entity = await azBlobStorageDatasetReader.Get(key);

        entity.Should().BeNull();
    }


    [Fact]
    public async Task Should_Batch_Get()
    {
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync();
        List<(string key, byte[] bytes)> entities = [];

        string parent = "testparent";
        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"{parent}/Should_Batch_Get_With_Parent_{i}", GenerateRandomData(UNIT_KB)));
        }
        await storageDatasetWriter.PutAll(entities.ToAsyncEnumerable());

        var batchResults = await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), parent)
            .ToListAsync();

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
        var (azBlobStorageDatasetReader, storageDatasetWriter) = await SetupStorageAsync();
        List<(string key, byte[] bytes)> entities = [];

        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Batch_Get_Deleted_Entity_{i}", GenerateRandomData(UNIT_KB)));
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

        var result = await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), null).ToListAsync();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        List<(string key, byte[] bytes)> entities = [];

        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Batch_Get_Deleted_Entity_{i}", GenerateRandomData(UNIT_KB)));
        }
        await azBlobStorageDatasetWriter.PutAll(entities.ToAsyncEnumerable());

        var nonExistentEntities = new List<string>()
        {
            "Should_Not_Batch_Get_Deleted_Entity_11",
            "Should_Not_Batch_Get_Deleted_Entity_12"
        };

        var result = await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key).Concat(nonExistentEntities), null).ToListAsync();

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
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Put entities with the same parent
        var bytes1 = GenerateRandomData(UNIT_MB, 1);
        await azBlobStorageDatasetWriter.Put("Parent1/Should_List_With_Parent_1", bytes1);

        var bytes2 = GenerateRandomData(UNIT_MB, 2);
        await azBlobStorageDatasetWriter.Put("Parent1/Should_List_With_Parent_2", bytes2);

        // Put entities with a different parent
        var bytes3 = GenerateRandomData(UNIT_MB, 3);
        await azBlobStorageDatasetWriter.Put("Parent2/Should_List_With_Parent_3", bytes3);

        // List entities with the matching parent
        var resultList = await azBlobStorageDatasetReader.List("Parent1").ToListAsync();

        resultList.Should().HaveCount(2);

        var entity1 = resultList.First(x => x.name == "Parent1/Should_List_With_Parent_1");
        var entity2 = resultList.First(x => x.name == "Parent1/Should_List_With_Parent_2");

        entity1.entityBytes.ToArray().Should().BeEquivalentTo(bytes1);
        entity2.entityBytes.ToArray().Should().BeEquivalentTo(bytes2);
    }

    [Fact]
    public async Task Should_Not_List_Without_Parent()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        await azBlobStorageDatasetWriter.Put("Parent1/Should_Not_List_Without_Parent_1", GenerateRandomData(UNIT_MB));
        await azBlobStorageDatasetWriter.Put("Parent2/Should_Not_List_Without_Parent_2", GenerateRandomData(UNIT_MB));

        var resultList = await azBlobStorageDatasetReader.List("Parent3").ToListAsync();
        resultList.Should().BeEmpty();

        var resultList2 = await azBlobStorageDatasetReader.List("").ToListAsync();
        resultList2.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Not_List_Deleted_Entities()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        string parent = "Parent";
        string key = $"{parent}/Should_Not_List_Deleted_Entities";
        await azBlobStorageDatasetWriter.Put(key, GenerateRandomData(UNIT_MB));

        await azBlobStorageDatasetWriter.Delete(key);
        var resultList = await azBlobStorageDatasetReader.List(parent).ToListAsync();
        resultList.Should().BeEmpty();
    }

    

    [Fact]
    public async Task Should_Get_Changed_Names_Updates()
    {
        //Should list updates after insert, then update.
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        var key1 = "Should_Get_Changed_Names_Updates_1";
        var key2 = "Should_Get_Changed_Names_Updates_2";

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 1));
        var changedNames01 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).changes.ToList();

        await azBlobStorageDatasetWriter.Put(key2, GenerateRandomData(UNIT_MB, 2));
        var changedNames02 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).changes.ToList();

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 3));
        var changedNames03 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).changes.ToList();

        var changedNames13 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 1)).changes.ToList();
        var changedNames23 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 2)).changes.ToList();

        changedNames01.Should().HaveCount(1);
        changedNames01.Find(x => x.name == key1).changeType.Should().Be(ChangeType.New);

        changedNames02.Should().HaveCount(2);
        changedNames02.Find(x => x.name == key1).changeType.Should().Be(ChangeType.New);
        changedNames02.Find(x => x.name == key2).changeType.Should().Be(ChangeType.New);

        changedNames03.Should().HaveCount(2);
        changedNames03.Find(x => x.name == key1).changeType.Should().Be(ChangeType.New);
        changedNames03.Find(x => x.name == key2).changeType.Should().Be(ChangeType.New);

        changedNames13.Should().HaveCount(2);
        changedNames13.Find(x => x.name == key1).changeType.Should().Be(ChangeType.Changed);
        changedNames13.Find(x => x.name == key2).changeType.Should().Be(ChangeType.New);

        changedNames23.Should().HaveCount(1);
        changedNames23.Find(x => x.name == key1).changeType.Should().Be(ChangeType.Changed);
    }

    [Fact]
    public async Task Should_Get_Changed_Names_Inserts()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        var key1 = "Should_Get_Changed_Names_Updates_1";
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 1));
        var changedNames01 =( await azBlobStorageDatasetReader.GetChangedNames(initialVersion)).changes.ToList();

        await azBlobStorageDatasetWriter.Delete(key1);
        var changedNames02 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 1)).changes.ToList();

        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 2));
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 3));
        await azBlobStorageDatasetWriter.Put(key1, GenerateRandomData(UNIT_MB, 4));
        var changedNames05 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 3)).changes.ToList();

        await azBlobStorageDatasetWriter.Delete(key1);
        var changedNames06 = (await azBlobStorageDatasetReader.GetChangedNames(initialVersion + 3)).changes.ToList();

        changedNames01.Should().HaveCount(1);
        changedNames01.Find(x => x.name == key1).changeType.Should().Be(ChangeType.New);

        changedNames02.Should().HaveCount(1);
        changedNames02.Find(x => x.name == key1).changeType.Should().Be(ChangeType.Deleted);

        changedNames05.Should().HaveCount(1);
        changedNames05.Find(x => x.name == key1).changeType.Should().Be(ChangeType.Changed);

        changedNames06.Should().HaveCount(1);
        changedNames06.Find(x => x.name == key1).changeType.Should().Be(ChangeType.Deleted);
    }

    [Fact]
    public async Task Should_Update_Latest_Version_Number_On_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();
        await azBlobStorageDatasetWriter.Put("Should_Update_Latest_Version_Number_On_Put", GenerateRandomData(UNIT_MB));

        var updatedVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion + 1);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Ignored_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        var bytes = GenerateRandomData(UNIT_MB);

        await azBlobStorageDatasetWriter.Put("Should_Not_Update_Latest_Version_Number_On_Ignored_Put", bytes);
        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetWriter.Put("Should_Not_Update_Latest_Version_Number_On_Ignored_Put", bytes);
        var updatedVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion);
    }

    [Fact]
    public async Task Should_Update_Latest_Version_Number_On_Put_All()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();
        var changes = new List<(string, byte[])>
        {
            ("Should_Update_Latest_Version_Number_On_Put_All_1", GenerateRandomData(UNIT_MB)),
            ("Should_Update_Latest_Version_Number_On_Put_All_2", GenerateRandomData(UNIT_MB)),
        };

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var updatedVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion + 1);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var changes = new List<(string, byte[])>
        {
            ("Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All_1", GenerateRandomData(UNIT_MB)),
            ("Should_Not_Update_Latest_Version_Number_On_Ignored_Put_All_2", GenerateRandomData(UNIT_MB)),
        };

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetWriter.PutAll(changes.ToAsyncEnumerable());
        var updatedVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        updatedVersion.Should().Be(initialVersion);
    }

    [Fact]
    public async Task Should_Not_Update_Latest_Version_Number_On_Read()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        List<(string key, byte[] bytes)> entities = [];
        for (int i = 0; i < 10; i++)
        {
            entities.Add(($"Should_Not_Update_Latest_Version_Number_On_Read_{i}", GenerateRandomData(UNIT_KB)));
        }
        await azBlobStorageDatasetWriter.PutAll(entities.ToAsyncEnumerable());
        var initialVersion = await azBlobStorageDatasetReader.GetLatestVersion();

        await azBlobStorageDatasetReader.Get("Should_Not_Update_Latest_Version_Number_On_Read_Non_Existing");
        (await azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.Get("Should_Not_Update_Latest_Version_Number_On_Read_1");
        (await azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.GetChangedNames(0);
        (await azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.BatchGet(entities.Select(x => x.key), null).ToListAsync();
        (await azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);

        await azBlobStorageDatasetReader.List("parent").ToListAsync();
        (await azBlobStorageDatasetReader.GetLatestVersion()).Should().Be(initialVersion);
    }



    [Fact]
    public async Task Should_Return_One_New_Entity_After_Put()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        var bytes = GenerateRandomData(UNIT_MB);
        var putResult = await azBlobStorageDatasetWriter.Put("AddingOneShouldLeadToListOfOneNewEntity", bytes);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).changes.ToList();


        // Then
        result.Where(x => x.changeType == ChangeType.New).Should().HaveCount(1);
        result.Where(x => x.changeType == ChangeType.New).Should().Contain(("AddingOneShouldLeadToListOfOneNewEntity", ChangeType.New));

        result.Where(x => x.changeType == ChangeType.Changed).Should().BeEmpty();
        result.Where(x => x.changeType == ChangeType.Deleted).Should().BeEmpty();
    }


    [Fact]
    public async Task Should_Return_One_New_Entity_After_Put_And_After_Waiting()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        CancellationToken cancellationToken = new();

        // Given
        var bytes = GenerateRandomData(UNIT_MB);
  
        Func<Task<ChangeSet>> action = async () => await azBlobStorageDatasetReader.WaitChangedNames(0, cancellationToken);
        var waitChangedListTask = action.Invoke();

        while (waitChangedListTask.Status != TaskStatus.WaitingForActivation)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }
        waitChangedListTask.Status.Should().Be(TaskStatus.WaitingForActivation);
       
        var putResult = await azBlobStorageDatasetWriter.Put("AddingOneShouldLeadToListOfOneNewEntity", bytes, cancellationToken);

        waitChangedListTask.Status.Should().Be(TaskStatus.RanToCompletion);

        // When
        var result = (await waitChangedListTask).changes.ToList();


        // Then
        result.Where(x => x.changeType == ChangeType.New).Should().HaveCount(1);
        result.Where(x => x.changeType == ChangeType.New).Should().Contain(("AddingOneShouldLeadToListOfOneNewEntity", ChangeType.New));

        result.Where(x => x.changeType == ChangeType.Changed).Should().BeEmpty();
        result.Where(x => x.changeType == ChangeType.Deleted).Should().BeEmpty();
    }
    
    [Fact]
    public async Task Should_Return_No_New_Entity_After_Canceling()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        CancellationTokenSource cancellationTokenSource = new();

        // Given
        var bytes = GenerateRandomData(UNIT_MB);

        Func<Task<ChangeSet>> action = async () => await azBlobStorageDatasetReader.WaitChangedNames(0, cancellationTokenSource.Token);
        var waitChangedListTask = action.Invoke();

        await Task.Delay(TimeSpan.FromSeconds(3), cancellationTokenSource.Token);
       
        waitChangedListTask.Status.Should().Be(TaskStatus.WaitingForActivation);
        await cancellationTokenSource.CancelAsync();

        await Task.Delay(TimeSpan.FromSeconds(2)); //wait to return from listTask

        waitChangedListTask.Status.Should().Be(TaskStatus.Canceled);
    }

    [Fact]
    public async Task Should_Return_Empty_Changelist_On_Delete_After_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToEmptyResultOverBothChanges";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));
        var deleteResult = await azBlobStorageDatasetWriter.Delete(name);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).changes.ToList();

        // Then
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Return_Single_New_In_Changelist_On_Update_After_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToEmptyResultOverBothChanges";
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(0)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_Changed_In_Changelist_On_Update_After_Insert_From_Insert()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "AddingOneThenChangingOneShouldLeadToListWithChangeOverTheLastDeltaOnly";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));
        var updateResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(putResult.NewDatasetVersion)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_Deleted_In_Changelist_On_Delete_After_Update()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "AddingOneThenDeletingOneShouldLeadToListWithDeleteOverTheLastDeltaOnly";
        var putResult = await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));
        var deleteResult = await azBlobStorageDatasetWriter.Delete(name);

        // When
        var result = (await azBlobStorageDatasetReader.GetChangedNames(putResult.NewDatasetVersion)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Deleted));
    }

    [Fact]
    public async Task Should_Return_Nothing_In_Changelist_On_FID()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Nothing_In_Changelist_On_FID";

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDID()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDID";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Deleted));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDI()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDI";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_FIDI()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_FIDI";

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFC";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_FIC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_FIC";

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.New));
    }

    [Fact]
    public async Task Should_Return_Single_In_Changelist_On_IFDIC()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        // Given
        string name = "Should_Return_Single_In_Changelist_On_IFDIC";

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //F
        var from = await azBlobStorageDatasetReader.GetLatestVersion();

        //D
        await azBlobStorageDatasetWriter.Delete(name);

        //I
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB));

        //C
        await azBlobStorageDatasetWriter.Put(name, GenerateRandomData(UNIT_MB, 1));

        //T
        var to = await azBlobStorageDatasetReader.GetLatestVersion();

        var result = (await azBlobStorageDatasetReader.GetChangedNames(from)).changes.ToList();

        // Then
        result.Should().ContainSingle();
        result.Should().Contain((name, ChangeType.Changed));
    }
}