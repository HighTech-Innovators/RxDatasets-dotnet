using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Builder;
using Google.Protobuf;
using Hightechinnovators.Dtos.DemodtoV1;
using HTI.RxDatasets;

namespace RxDatasets.DemodtoV1.Server;

public static class DI
{
    public static void ConfigureServices(IServiceCollection builderServices)
    {
        builderServices.AddSingletonsForDataset<Demodto>();
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