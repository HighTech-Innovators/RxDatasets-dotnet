using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.AzBlob;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<AzStorageConfig>();
        builderServices.AddSingleton<IObjectStorageProvider<TEntity>, AzStorageProvider<TEntity>>();
    }
}