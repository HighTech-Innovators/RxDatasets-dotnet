namespace RxDatasets.Abstractions;

public interface IObjectStorageProvider<T>
{
    /// <summary>
    /// Download a file from storage provider.
    /// </summary>
    /// <param name="objectKey">The name of the file on provider</param>
    /// <returns>An array of bytes of the file, from position 0</returns>
    /// <exception cref="InvalidOperationException">In case no connection to the provider can be made or other error</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist</exception>
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey);

    /// <summary>
    /// Download a file from storage provider, based on a position and length.
    /// </summary>
    /// <param name="objectKey">The name of the file on provider</param>
    /// <param name="position">The positions where to start the download</param>
    /// <param name="length">The number of bytes to download</param>
    /// <returns>The bytes of the object from the position and for length</returns>
    /// <exception cref="InvalidOperationException">In case no connection to the provider can be made or other error</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist</exception>
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length);

    /// <summary>
    /// Download a file from storage provider.
    /// </summary>
    /// <param name="objectKey">The name of the file on provider</param>
    /// <param name="callback">The action to apply for the stream</param>
    /// <returns>Nothing</returns>
    /// <exception cref="InvalidOperationException">In case no connection to the provider can be made or other error</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist</exception>
    public Task DownloadWithCallBack(string objectKey, Action<Stream> callback);
    public Task UploadBytes(string objectKey, byte[] bytes);
    public Task UploadFileStream(string objectKey, FileStream stream);
    public Task DeleteFile(string objectKey);
    public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix);
}
