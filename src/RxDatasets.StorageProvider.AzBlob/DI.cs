using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.AzBlob
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithAzureStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<AzStorageConfig>();
            builder.Services.AddSingleton<IObjectStorageProvider<TEntity>, AzStorageProvider<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<AzHealthCheck<TEntity>>($"storage.azblob.{typeof(TEntity).Name}");

            return builder;
        }
    }
}