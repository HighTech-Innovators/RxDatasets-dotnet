using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.S3
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithS3Storage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<S3Config>();
            builder.Services.AddSingleton<IObjectStorageProvider<TEntity>, S3StorageProvider<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<S3HealthCheck<TEntity>>($"storage.s3.{typeof(TEntity).Name}");

            return builder;
        }
    }
}