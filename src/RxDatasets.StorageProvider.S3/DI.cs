using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.S3;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<S3Config>();
        builderServices
            .AddSingleton<IObjectStorageProvider<TEntity>, S3StorageProvider<TEntity>>();
    }
}