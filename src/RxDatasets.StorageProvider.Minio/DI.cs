using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.Minio
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithMinioStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<MinioS3Config>();
            builder.Services.AddSingleton<IDiskStorage<TEntity>, MinioS3Storage<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<MinioS3HealthCheck<TEntity>>($"storage.minio.{typeof(TEntity).Name}");

            return builder;
        }
    }
}
