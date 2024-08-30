using Google.Protobuf;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting;

public class ProtobufJsonToObjectStorageReporter<T>(
    IObjectStorageProvider<T> storageProvider,
    IOptions<JsonOutputConfig> reportingConfig)
    : IDatasetUpdateReporting<T>
    where T : IMessage<T>
{
    private readonly JsonFormatter _jsonFormatter = new(JsonFormatter.Settings.Default.WithIndentation());
    private readonly MessageParser<T> _protoParser = new(() => (T)Activator.CreateInstance(typeof(T))!);
    private readonly bool _includeVersionInFileName = reportingConfig.Value.IncludeVersionInFileName;
    private readonly bool _includeVersionInPath = reportingConfig.Value.IncludeVersionInPath;

    public Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(deltaVersion);

    public async Task OnNewDto(string name, byte[] newProtoBytes, object? version)
    {
        var customObjectKey = GetObjectKey(name, version, "created/");
        var jsonBytes = ProtoBytesToJsonBytes(newProtoBytes);
        await storageProvider.UploadBytes(customObjectKey, jsonBytes);
    }

    public async Task OnChangedDto(string name, byte[] changedProtoBytes, object? version)
    {
        var customObjectKey = GetObjectKey(name, version, "changed/");
        var jsonBytes = ProtoBytesToJsonBytes(changedProtoBytes);
        await storageProvider.UploadBytes(customObjectKey, jsonBytes);
    }

    public async Task OnDeletedDto(string name, object? version)
    {
        var customObjectKey = GetObjectKey(name, version, "deleted/");
        await storageProvider.UploadBytes(customObjectKey, "{}"u8.ToArray());
    }

    public string ReportingIdentifier => "json-entity-writer";

    private string GetObjectKey(string name, object? version, string changeDir)
    {
        var prefix = reportingConfig.Value.Prefix;
        var versionDir = _includeVersionInPath ? $"{version}/" : "";
        var versionSuffix = _includeVersionInFileName ? $".{version}" : "";
        return $"{prefix}{versionDir}{changeDir}{name}{versionSuffix}.json";
    }

    private byte[] ProtoBytesToJsonBytes(byte[] protoBytes)
    {
        var entity = _protoParser.ParseFrom(protoBytes);
        var jsonString = _jsonFormatter.Format(entity);
        var jsonBytes = System.Text.Encoding.UTF8.GetBytes(jsonString);
        return jsonBytes;
    }
}