using Google.Protobuf;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting
{
    public class ProtobufJsonToObjectStorageReporter<T>(
        IObjectStorageProvider<T> storageProvider,
        IOptions<ReportingConfig> reportingConfig)
        : IDatasetUpdateReporting<T>
        where T : IMessage<T>
    {
        private readonly JsonFormatter _jsonFormatter = new(JsonFormatter.Settings.Default.WithIndentation());
        private readonly MessageParser<T> _protoParser = new(() => (T)Activator.CreateInstance(typeof(T))!);
        private readonly bool _insertVersionToFileName = reportingConfig.Value.InsertVersionToFileName;
        private readonly bool _insertVersionToPrefix = reportingConfig.Value.InsertVersionToPrefix;

        public Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(deltaVersion);

        public Task CommitVersionTransaction(object state)
        {
            return Task.CompletedTask;
        }

        public Task RollBack(object state)
        {
            return Task.CompletedTask;
        }

        public Task OnNewDto(string name, byte[] newProtoBytes, object? optionalTransaction)
        {
            var version = (ulong)optionalTransaction!;
            var customObjectKey = CustomObjectKey(name, version);
            var data = _protoParser.ParseFrom(newProtoBytes);
            var jsonMessage = _jsonFormatter.Format(data);
            storageProvider.UploadBytes(customObjectKey, System.Text.Encoding.UTF8.GetBytes(jsonMessage));
            return Task.CompletedTask;
        }

        public Task OnChangedDto(string name, byte[] newProtoBytes, object? optionalTransaction)
        {
            var version = (ulong)optionalTransaction!;
            var customObjectKey = CustomObjectKey(name, version);
            var data = _protoParser.ParseFrom(newProtoBytes);
            var jsonMessage = _jsonFormatter.Format(data);
            storageProvider.UploadBytes(customObjectKey, System.Text.Encoding.UTF8.GetBytes(jsonMessage));
            return Task.CompletedTask;
        }
        public Task OnDeletedDto(string name, object? optionalTransaction)
        {
            var version = (ulong)optionalTransaction!;
            var customObjectKey = CustomObjectKey(name, version);
            storageProvider.UploadBytes(customObjectKey, System.Text.Encoding.UTF8.GetBytes(string.Empty));
            return Task.CompletedTask;
        }

        private string CustomObjectKey(string name, ulong version)
        {
            return $"{reportingConfig.Value.Prefix}{(_insertVersionToPrefix ? $"{version}/" : "")}changed/{name}{(_insertVersionToFileName ? $".{version}" : "")}.json";
        }
    }
}
