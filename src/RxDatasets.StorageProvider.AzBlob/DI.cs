using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.AzBlob;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<AzStorageConfig>();
        builderServices.AddSingleton<IObjectStorageProvider<TEntity>, AzStorageProvider<TEntity>>();
        builderServices.AddHealthChecks().AddCheck<AzHealthCheck<TEntity>>($"storage.azblob.{typeof(TEntity).Name}");
    }
}