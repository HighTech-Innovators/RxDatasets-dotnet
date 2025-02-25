using Google.Protobuf;
using Hightechinnovators.Dtos.TestdtoV1;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace RxDatasets.TestdtoV1.Server;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    {
        builderServices.AddSingletonsForDataset<Testdto>();
    }

    public static void ConfigureApp(WebApplication app)
    {
        app.MapGrpcService<WriterServiceImpl>();
        app.MapGrpcService<ReaderServiceImpl>();
    }

    private static void AddSingletonsForDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        services
            .AddSingleton<GrpcReaderServiceImpl<TEntity>>();
    }
}
