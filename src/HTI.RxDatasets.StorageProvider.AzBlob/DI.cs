using HTI.RxDatasets.Abstractions;
using Microsoft.Extensions.DependencyInjection;
namespace HTI.RxDatasets.StorageProvider.AzBlob;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<AzStorageConfig>();
        builderServices.AddSingleton<IObjectStorageProvider<TEntity>, AzStorageProvider<TEntity>>();
    }
}