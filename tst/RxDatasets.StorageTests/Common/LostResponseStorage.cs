using RxDatasets.Abstractions;

namespace RxDatasets.StorageTests.Common;

/// <summary>
/// Storage whose first conditional write to a matching key lands but reports a failed precondition, as when the SDK
/// retries a write whose response was lost and the retry finds the object already written.
/// </summary>
public sealed class LostResponseStorage<T>(IConditionalDiskStorage<T> inner, Func<string, bool> matches)
    : IConditionalDiskStorage<T>
{
    private int _lost;

    public async Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion)
    {
        var version = await inner.TryUploadBytes(objectKey, bytes, expectedVersion);
        return version != null && matches(objectKey) && Interlocked.Exchange(ref _lost, 1) == 0 ? null : version;
    }

    public Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey) =>
        inner.DownloadBytesWithVersion(objectKey);
    public Task<bool> TryCreateFromStream(string objectKey, Stream stream) => inner.TryCreateFromStream(objectKey, stream);
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey) => inner.DownloadBytes(objectKey);
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length) =>
        inner.DownloadBytes(objectKey, position, length);
    public Task DownloadWithCallBack(string objectKey, Action<Stream> callback) =>
        inner.DownloadWithCallBack(objectKey, callback);
    public Task UploadBytes(string objectKey, byte[] bytes) => inner.UploadBytes(objectKey, bytes);
    public Task UploadFileStream(string objectKey, Stream stream) => inner.UploadFileStream(objectKey, stream);
    public Task DeleteFile(string objectKey) => inner.DeleteFile(objectKey);
    public Task DeleteFolder(string prefix) => inner.DeleteFolder(prefix);
    public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) => inner.ListObjects(prefix);
    public Task<long> GetObjectContentLength(string objectKey) => inner.GetObjectContentLength(objectKey);
}
