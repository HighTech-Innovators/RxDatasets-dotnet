using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithGoogleCloudStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<GoogleCloudConfig>();
            builder.Services.TryAddSingleton<IDiskStorage<TEntity>, GoogleCloudStorage<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<GoogleCloudHealthCheck<TEntity>>($"storage.googlecloud.{typeof(TEntity).Name}", tags: ["googlecloud", typeof(TEntity).Name]);
            return builder;
        }
    }
}
