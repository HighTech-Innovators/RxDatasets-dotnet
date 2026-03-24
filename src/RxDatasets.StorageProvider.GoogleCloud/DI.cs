using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithGoogleCloudStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<GoogleCloudConfig>();
            builder.WithCustomStorageRegistration((services, readOnlyMode, withCache) =>
            {
                services.TryAddSingleton<IDiskStorage<TEntity>, GoogleCloudStorage<TEntity>>();
                services.TryAddSingleton<ObjectStorageReader<TEntity>>();
                services.TryAddSingleton<IVersionedArtifactRowStore, GoogleCloudVersionedArtifactRowStore>();
                services.TryAddSingleton<ILockProvider, BigtableLockProvider<TEntity>>();
                services.AddSingleton<IStorageReader<TEntity>, HybridBigtableStorageReader<TEntity>>();

                if (readOnlyMode)
                {
                    services.AddSingletonAsHostedService<ReadOnlyHybridBigtableStorageProvider<TEntity>>();

                    if (withCache)
                    {
                        services.AddValidatedConfigSection<DtoStorageCacheConfig>();
                        services.AddSingletonAsHostedService<IReadOnlyStorageProvider<TEntity>,
                            ReadOnlyCacheStorageProvider<TEntity, ReadOnlyHybridBigtableStorageProvider<TEntity>>>();
                    }
                    else
                    {
                        services.AddSingletonAlias<IReadOnlyStorageProvider<TEntity>, ReadOnlyHybridBigtableStorageProvider<TEntity>>();
                    }

                    return;
                }

                services.AddSingletonAsHostedService<HybridBigtableStorageProvider<TEntity>>();

                if (withCache)
                {
                    services.AddValidatedConfigSection<DtoStorageCacheConfig>();
                    services.AddSingletonAsHostedService<IStorageProvider<TEntity>,
                        CacheStorageProvider<TEntity, HybridBigtableStorageProvider<TEntity>>>();
                }
                else
                {
                    services.AddSingletonAlias<IStorageProvider<TEntity>, HybridBigtableStorageProvider<TEntity>>();
                }

                services.AddSingletonAlias<IReadOnlyStorageProvider<TEntity>, IStorageProvider<TEntity>>();
            });
            builder.Services.AddHealthChecks().AddCheck<GoogleCloudHealthCheck<TEntity>>($"storage.googlecloud.{typeof(TEntity).Name}", tags: ["googlecloud", typeof(TEntity).Name]);
            return builder;
        }

        public static DtoServerBuilder<TEntity> WithGoogleCloudBlobOnlyStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<GoogleCloudConfig>();
            builder.Services.TryAddSingleton<IDiskStorage<TEntity>, GoogleCloudStorage<TEntity>>();
            return builder;
        }
    }
}
