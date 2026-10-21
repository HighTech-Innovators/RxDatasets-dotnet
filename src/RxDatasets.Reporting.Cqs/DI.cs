using Google.Protobuf;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

namespace RxDatasets.Reporting.Cqs;

public static class DtoServerBuilderExtensions
{
    public static DtoServerBuilder<TEntity> WithCqsReporting<TEntity>(this DtoServerBuilder<TEntity> builder)
        where TEntity : class, IMessage<TEntity>, new()
    {
        builder.Services.AddValidatedConfigSection<CqsReportingConfig>();
        builder.Services.AddGrpcClient<CqsIngressPushService.CqsIngressPushServiceClient>((services, options) =>
        {
            options.Address = new Uri(services.GetRequiredService<IOptions<CqsReportingConfig>>().Value.Endpoint);
        });
        builder.Services.AddSingleton<CqsReporter<TEntity>>();
        builder.Services.AddSingleton<BatchingDatasetUpdateReportingPlugin<TEntity, CqsReporter<TEntity>>>();
        builder.Services
            .AddHostedService<DatasetUpdateConsumingService<TEntity, BatchingDatasetUpdateReportingPlugin<TEntity, CqsReporter<TEntity>>>>();

        return builder;
    }
}