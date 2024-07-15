using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.InMemoryDataset;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices) where TEntity : class, IMessage<TEntity>
    {
        builderServices.AddSingletonsForDataset<TEntity>();
    }

    private static void AddSingletonsForDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        var inMemoryDataset = new InMemoryDataset<TEntity>();

        services
            .AddSingleton<IDatasetReader<TEntity>>(inMemoryDataset)
            .AddSingleton<IDatasetWriter<TEntity>>(inMemoryDataset);
    }
}
