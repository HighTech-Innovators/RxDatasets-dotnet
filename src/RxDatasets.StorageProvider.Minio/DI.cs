using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.Minio;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<MinioS3Config>();
        builderServices
            .AddSingleton<IObjectStorageProvider<TEntity>, MinioS3StorageProvider<TEntity>>();
        builderServices.AddHealthChecks().AddCheck<MinioS3HealthCheck<TEntity>>($"storage.minio.{typeof(TEntity).Name}");
    }
}