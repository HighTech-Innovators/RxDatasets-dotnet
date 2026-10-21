using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using RxDatasets;
using RxDatasets.BackgroundServices;
using RxDatasets.Reporting.Cqs;
using RxDatasets.StorageProvider.S3;

namespace Host;

public static class DI
{
    private const string ReadinessTags = "readiness";

    /// <summary>
    /// Reporting to the Change Queue Service is opt-in: set <c>CqsReporting:Endpoint</c> (environment variable
    /// <c>CqsReporting__Endpoint</c>). Without it the Host is what it always was: Demodto only, no CQS.
    /// With it, the Host also serves Demodto2 and reports both types.
    /// </summary>
    public static bool CqsReportingEnabled(IConfiguration? configuration) =>
        !string.IsNullOrWhiteSpace(configuration?["CqsReporting:Endpoint"]);

    public static void ConfigureServices(IServiceCollection builderServices, IConfiguration? configuration = null)
    {
        bool cqs = CqsReportingEnabled(configuration);

        RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        var demodto = builderServices.AddDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>()
            .WithS3Storage().WithDistributedLock().WithCache().WithSnapshot().WithCleanup();
        if (cqs)
        {
            demodto.WithCqsReporting();
        }

        demodto.Build(ReadinessTags);

        if (cqs)
        {
            RxDatasets.Demodto2V1.Server.DI.ConfigureServices(builderServices);
            builderServices.AddDtoServer<Hightechinnovators.Dtos.Demodto2V1.Demodto2>()
                .WithS3Storage().WithDistributedLock().WithCache().WithSnapshot().WithCleanup()
                .WithCqsReporting().Build(ReadinessTags);
        }

        //new testdto
        //RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        //builderServices.AddDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>().WithLocalDiskStorage().WithCache(readOnly: true).BuildReadOnly();

        builderServices.AddGrpc();
        builderServices.AddGrpcReflection();

        builderServices.AddSingleton<ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>();
        builderServices.AddSingleton<DatasetUpdateReportingPlugin<Hightechinnovators.Dtos.DemodtoV1.Demodto, ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>>();
        builderServices.AddHostedService<DatasetUpdateConsumingService<Hightechinnovators.Dtos.DemodtoV1.Demodto, DatasetUpdateReportingPlugin<Hightechinnovators.Dtos.DemodtoV1.Demodto, ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>>>();
    }

    public static void ConfigureApp(WebApplication app)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureApp(app);
        if (CqsReportingEnabled(app.Configuration))
        {
            RxDatasets.Demodto2V1.Server.DI.ConfigureApp(app);
        }

        RxDatasets.TestdtoV1.Server.DI.ConfigureApp(app);

        app.MapGrpcReflectionService();
        app.UseRouting();
        app.UseGrpcWeb(new GrpcWebOptions { DefaultEnabled = true });

        app.MapHealthChecks("/healthz", new HealthCheckOptions
        {
            Predicate = check => !check.Tags.Contains(ReadinessTags)
        });

        app.MapHealthChecks("/readiness", new HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains(ReadinessTags)
        });


        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
    }
}
