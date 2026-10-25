using FluentAssertions;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageTests.Common;

/// <summary>
/// Behaviour every <see cref="IConditionalDiskStorage{T}"/> must have for the distributed lock to be safe.
/// </summary>
public static class ConditionalStorageContract
{
    public static async Task VerifyAsync(IConditionalDiskStorage<object> storage)
    {
        var key = $"cas/{Guid.NewGuid():N}";

        await storage.Awaiting(s => s.DownloadBytesWithVersion(key)).Should().ThrowAsync<FileNotFoundException>();
        (await storage.TryUploadBytes(key, [1], "1234")).Should().BeNull("an absent object has no version to match");

        var v1 = await storage.TryUploadBytes(key, [1], null);
        v1.Should().NotBeNull("create-only succeeds on an absent object");
        (await storage.TryUploadBytes(key, [2], null)).Should().BeNull("create-only fails on an existing object");

        var (bytes, version) = await storage.DownloadBytesWithVersion(key);
        bytes.ToArray().Should().Equal(1);
        version.Should().Be(v1, "the version returned by a write is the one a read reports");

        var v2 = await storage.TryUploadBytes(key, [3], v1);
        v2.Should().NotBeNull("replace succeeds on the current version").And.NotBe(v1);
        (await storage.TryUploadBytes(key, [4], v1)).Should().BeNull("replace fails on a stale version");
        (await storage.DownloadBytesWithVersion(key)).Bytes.ToArray().Should().Equal(3);

        await storage.UploadBytes(key, [5]);
        (await storage.TryUploadBytes(key, [6], v2)).Should().BeNull("an unconditional write also changes the version");

        // Concurrent creates: exactly one wins
        var raceKey = $"cas/{Guid.NewGuid():N}";
        var results = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(i => Task.Run(() => storage.TryUploadBytes(raceKey, [(byte)i], null))));
        results.Count(r => r != null).Should().Be(1);
    }
}
