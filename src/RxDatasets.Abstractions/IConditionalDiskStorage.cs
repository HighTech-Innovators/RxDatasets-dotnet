namespace RxDatasets.Abstractions;

/// <summary>
/// Compare-and-swap operations on single objects, needed to coordinate multiple instances
/// (e.g. the distributed lock). Versions are opaque tokens (ETag, generation, content hash):
/// only compare them for equality and pass them back to <see cref="TryUploadBytes"/>.
/// Writes through this interface are never queued or buffered.
/// </summary>
public interface IConditionalDiskStorage<T> : IDiskStorage<T>
{
    /// <summary>
    /// Download an object together with its current version.
    /// </summary>
    /// <exception cref="FileNotFoundException">In case the object does not exist</exception>
    Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey);

    /// <summary>
    /// Write an object only if its current version equals <paramref name="expectedVersion"/>,
    /// or, when <paramref name="expectedVersion"/> is null, only if it does not exist yet.
    /// </summary>
    /// <returns>The new version, or null if the precondition did not hold (nothing was written)</returns>
    Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion);
}
