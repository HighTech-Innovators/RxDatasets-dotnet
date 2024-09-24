using RxDatasets;
using RxDatasets.BackgroundServices;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProvider.Minio;

namespace Host;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    { 
        RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        builderServices.AddDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>().WithMinioStorage().WithCache().WithSnapshot().WithCleanup().Build();

        //new testdto
        RxDatasets.TestdtoV1.Server.DI.ConfigureServices(builderServices);
        builderServices.AddDtoServer<Hightechinnovators.Dtos.TestdtoV1.Testdto>().WithMinioStorage().WithCache().WithSnapshot().WithCleanup().Build();

        builderServices.AddGrpc();
        builderServices.AddGrpcReflection();

        builderServices.AddSingleton<ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>();
        builderServices.AddSingleton<DatasetUpdateReportingPlugin<Hightechinnovators.Dtos.DemodtoV1.Demodto, ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>>();
        builderServices.AddHostedService<DatasetUpdateConsumingService<Hightechinnovators.Dtos.DemodtoV1.Demodto, DatasetUpdateReportingPlugin<Hightechinnovators.Dtos.DemodtoV1.Demodto, ConsoleReporter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>>>();
    }

    public static void ConfigureApp(WebApplication app)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureApp(app);
        RxDatasets.TestdtoV1.Server.DI.ConfigureApp(app);

        app.MapGrpcReflectionService();
        app.UseRouting();
        app.UseGrpcWeb(new GrpcWebOptions { DefaultEnabled = true });
        app.MapHealthChecks("/healthz");

        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
    }
}