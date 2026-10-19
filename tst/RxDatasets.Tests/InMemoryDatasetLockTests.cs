using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// Every operation on an <see cref="InMemoryDataset{T}"/> takes the same
/// semaphore; each one must give it back, or the next operation waits forever.
/// </summary>
public class InMemoryDatasetLockTests
{
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(5);

    private static async Task<InMemoryDataset<byte[]>> DatasetWith(params string[] names)
    {
        var dataset = new InMemoryDataset<byte[]>();
        foreach (string name in names)
        {
            await dataset.Put(name, [1], null, null);
        }

        return dataset;
    }

    [Fact]
    public async Task List_ThenGet_ShouldNotWaitForever()
    {
        // Arrange
        InMemoryDataset<byte[]> dataset = await DatasetWith("parent/a", "parent/b");

        // Act
        (List<string> names, ulong _) = await dataset.List("parent").WaitAsync(Soon);
        (string name, ReadOnlyMemory<byte> entityBytes)? entity = await dataset.Get("parent/a").WaitAsync(Soon);

        // Assert
        names.Should().BeEquivalentTo("parent/a", "parent/b");
        entity.Should().NotBeNull();
    }

    [Fact]
    public async Task List_Twice_ThenPut_ShouldNotWaitForever()
    {
        // Arrange
        InMemoryDataset<byte[]> dataset = await DatasetWith("parent/a");

        // Act
        await dataset.List("parent").WaitAsync(Soon);
        await dataset.List("").WaitAsync(Soon);
        await dataset.Put("parent/b", [2], null, null).WaitAsync(Soon);

        // Assert
        (await dataset.GetLatestVersion().WaitAsync(Soon)).Should().Be(2);
    }

    [Fact]
    public async Task ObserveDatasetChanges_ThenPut_ShouldNotWaitForever()
    {
        // Arrange
        InMemoryDataset<byte[]> dataset = await DatasetWith("parent/a");

        // Act
        using IDisposable subscription = dataset.ObserveDatasetChanges(0).Subscribe(_ => { });
        await dataset.Put("parent/b", [2], null, null).WaitAsync(Soon);

        // Assert
        (await dataset.GetLatestVersion().WaitAsync(Soon)).Should().Be(2);
    }
}
