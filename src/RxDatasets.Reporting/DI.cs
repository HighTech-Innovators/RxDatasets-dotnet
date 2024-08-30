using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

namespace RxDatasets.Reporting;

public static class DI
{
    public static void AddJsonOutputWriting<TEntity>(this IServiceCollection builderServices) where TEntity : IMessage<TEntity>
    {
        builderServices.AddValidatedConfigSection<JsonOutputConfig>();
        builderServices.AddSingleton<ProtobufJsonToObjectStorageReporter<TEntity>>();
        builderServices.AddSingleton<DatasetUpdateReportingPlugin<TEntity, ProtobufJsonToObjectStorageReporter<TEntity>>>();
        builderServices.AddHostedService<DatasetUpdateConsumingService<TEntity, DatasetUpdateReportingPlugin<TEntity, ProtobufJsonToObjectStorageReporter<TEntity>>>>();
    }
}