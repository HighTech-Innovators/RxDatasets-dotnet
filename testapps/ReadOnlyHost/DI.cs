using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets;
using RxDatasets.StorageProvider.S3;

namespace ReadOnlyHost;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        builderServices.AddReadOnlyDtoServer<Hightechinnovators.Dtos.DemodtoV1.Demodto>().WithS3Storage().WithCache().Build();

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
    }
}
