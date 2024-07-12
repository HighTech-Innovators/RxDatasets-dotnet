namespace Host.Testserver;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureServices(builderServices);
        RxDatasets.TestdtoV1.Server.DI.ConfigureServices(builderServices);
        HTI.RxDatasets.InMemoryDataset.DI.ConfigureServices<Hightechinnovators.Dtos.DemodtoV1.Demodto>(builderServices);
        HTI.RxDatasets.InMemoryDataset.DI.ConfigureServices<Hightechinnovators.Dtos.TestdtoV1.Testdto>(builderServices);

        builderServices.AddGrpc();
        builderServices.AddGrpcReflection();
    }

    public static void ConfigureApp(WebApplication app)
    {
        RxDatasets.DemodtoV1.Server.DI.ConfigureApp(app);
        RxDatasets.TestdtoV1.Server.DI.ConfigureApp(app);

        app.MapGrpcReflectionService();
        app.UseRouting();
        app.UseGrpcWeb(new GrpcWebOptions { DefaultEnabled = true });

        app.MapGet("/", () => "Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
    }
}