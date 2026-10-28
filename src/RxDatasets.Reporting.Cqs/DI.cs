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
        // A singleton: it keeps the one client it is given, so the client factory's handler rotation (which picks up DNS
        // changes) does not apply to it. That is fine: when the connection to CQS closes, as it does when CQS restarts or
        // moves, the next one resolves the endpoint again.
        builder.Services.AddSingleton<CqsReporter<TEntity>>();
        builder.Services.AddSingleton<DatasetUpdateReportingPlugin<TEntity, CqsReporter<TEntity>>>();
        // Last, so it stops before the lock and storage: its cursor write after a durable push needs both.
        return builder.RegisterLast(services => services
            .AddHostedService<
                DatasetUpdateConsumingService<TEntity, DatasetUpdateReportingPlugin<TEntity, CqsReporter<TEntity>>>>());
    }
}