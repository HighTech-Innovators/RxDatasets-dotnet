using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.InMemoryDataset;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices) where TEntity : class, IMessage<TEntity>
    {
        builderServices.AddSingletonsForDataset<TEntity>();
    }

    private static void AddSingletonsForDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        services
            .AddSingleton(sp =>
                new InMemoryDataset<TEntity>(sp.GetService<IOptions<AliasOptions<TEntity>>>()?.Value.Definitions))
            .AddSingleton<IDatasetReader<TEntity>>(sp => sp.GetRequiredService<InMemoryDataset<TEntity>>())
            .AddSingleton<IDatasetWriter<TEntity>>(sp => sp.GetRequiredService<InMemoryDataset<TEntity>>());
    }
}
