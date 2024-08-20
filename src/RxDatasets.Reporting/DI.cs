using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices) where TEntity : IMessage<TEntity>
    {
        builderServices.AddValidatedConfigSection<ReportingConfig>();
        builderServices.AddSingleton<IDatasetUpdateReporting<TEntity>, ProtobufJsonToObjectStorageReporter<TEntity>>();
    }
}