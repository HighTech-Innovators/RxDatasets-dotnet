using RxDatasets;
using RxDatasets.Abstractions;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace Host.Testserver;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        builderServices.AddDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>().WithDistributedLock().WithSnapshot().WithCleanup().Build();
        //builderServices.AddReadOnlyDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>().WithCache().Build();
        builderServices.AddSingleton<IDiskStorage<Hightechinnovators.Dtos.DemodtoV1.Demodto>, LowLevelInMemoryStorage<Hightechinnovators.Dtos.DemodtoV1.Demodto>>();

        builderServices.AddGrpc();
        builderServices.AddGrpcReflection();
    }

    public static void ConfigureApp(WebApplication app)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureApp(app);

        app.MapGrpcReflectionService();
        app.UseRouting();
        app.UseGrpcWeb(new GrpcWebOptions { DefaultEnabled = true });

        app.MapHealthChecks("/healthz");
        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");

        app.Services.GetRequiredService<IDatasetReader<Hightechinnovators.Dtos.DemodtoV1.Demodto>>();
        app.Services.GetRequiredService<IDatasetWriter<Hightechinnovators.Dtos.DemodtoV1.Demodto>>();
    }
}
