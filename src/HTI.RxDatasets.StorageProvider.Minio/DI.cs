using HTI.RxDatasets.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace HTI.RxDatasets.StorageProvider.Minio;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices)
    {
        builderServices.AddValidatedConfigSection<MinioS3Config>();
        builderServices
            .AddSingleton<IObjectStorageProvider<TEntity>, MinioS3StorageProvider<TEntity>>();
    }
}