using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

namespace RxDatasets.Reporting
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithJsonReporting<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<JsonOutputConfig>();
            builder.Services.AddSingleton<ProtobufJsonToObjectStorageReporter<TEntity>>();
            builder.Services.AddSingleton<DatasetUpdateReportingPlugin<TEntity, ProtobufJsonToObjectStorageReporter<TEntity>>>();
            builder.Services.AddHostedService<DatasetUpdateConsumingService<TEntity, DatasetUpdateReportingPlugin<TEntity, ProtobufJsonToObjectStorageReporter<TEntity>>>>();

            return builder;
        }
    }
}