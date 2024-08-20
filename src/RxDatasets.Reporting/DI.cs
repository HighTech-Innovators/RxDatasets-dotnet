using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting;

public static class DI
{
    public static void AddJsonOutputWriting<TEntity>(this IServiceCollection builderServices) where TEntity : IMessage<TEntity>
    {
        builderServices.AddValidatedConfigSection<JsonOutputConfig>();
        builderServices.AddSingleton<IDatasetUpdateReporting<TEntity>, ProtobufJsonToObjectStorageReporter<TEntity>>();
    }
}