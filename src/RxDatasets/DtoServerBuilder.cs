using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;
using System.Reactive.Concurrency;
using Google.Protobuf;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets
{
    public static class DtoServerServiceCollectionExtensions
    {
        public static DtoServerBuilder<TEntity> AddDtoServer<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
        {
            services.AddSingleton(serviceProvider => serviceProvider);
            // Initialize the DtoServerBuilder with the provided IServiceCollection
            return new DtoServerBuilder<TEntity>(services);
        }
        
        public static DtoServerBuilder<TEntity> AddReadOnlyDtoServer<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
        {
            services.AddSingleton(serviceProvider => serviceProvider);
            return new DtoServerBuilder<TEntity>(services, true);
        }
    }

    public class DtoServerBuilder<TEntity>(IServiceCollection services, bool readOnlyMode = false)
        where TEntity : class, IMessage<TEntity>
    {
        private bool _isCacheConfigured;
        private bool _isInMemoryDataset;

        public IServiceCollection Services => services;

        public DtoServerBuilder<TEntity> WithCache()
        {
            if (_isInMemoryDataset)
            {
               return this;
            }
            
            services.AddValidatedConfigSection<StorageConfig>();

            services.AddValidatedConfigSection<DtoStorageCacheConfig>();
            services.AddSingleton<IStorageReader<TEntity>, ObjectStorageReader<TEntity>>();
            _isCacheConfigured = true;
            
            
            if (readOnlyMode)
            {
                
                AddReadOnlyCachingDI();
            }
            else
            {
                AddReadWriteCachingDI();
            }

            return this;
        }

        private void AddReadOnlyCachingDI()
        {            
            services.AddSingleton<ReadOnlyObjectStorageProvider<TEntity>>();
            services.AddSingleton<IReadOnlyStorageProvider<TEntity>>(sp =>
                new ReadOnlyCacheStorageProvider<TEntity>(
                    sp.GetRequiredService<ReadOnlyObjectStorageProvider<TEntity>>(),
                    sp.GetRequiredService<IOptions<DtoStorageCacheConfig>>(),
                    sp.GetRequiredService<INotificationsHub<TEntity>>(),
                    sp.GetRequiredService<ILogger<ReadOnlyCacheStorageProvider<TEntity>>>()
                ));

            services.AddHostedService(sp => sp.GetRequiredService<ReadOnlyObjectStorageProvider<TEntity>>()); 
            services.AddHostedService(sp =>
                (ReadOnlyCacheStorageProvider<TEntity>)sp.GetRequiredService<IReadOnlyStorageProvider<TEntity>>());

        }

        private void AddReadWriteCachingDI()
        {
            services.AddSingleton<IReadOnlyStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
            services.AddHostedService(sp =>
                (ReadOnlyObjectStorageProvider<TEntity>)sp.GetRequiredService<IReadOnlyStorageProvider<TEntity>>());
            
            services.AddSingleton<ObjectStorageProvider<TEntity>>();
            services.AddHostedService(sp => sp.GetRequiredService<ObjectStorageProvider<TEntity>>());
            services.AddSingleton<IStorageProvider<TEntity>>(sp =>
                new CacheStorageProvider<TEntity>(
                    sp.GetRequiredService<ObjectStorageProvider<TEntity>>(),
                    sp.GetRequiredService<IOptions<DtoStorageCacheConfig>>(),
                    sp.GetRequiredService<ILogger<CacheStorageProvider<TEntity>>>(),
                    sp.GetRequiredService<INotificationsHub<TEntity>>(),
                    sp.GetRequiredService<ObjectStorageProvider<TEntity>>()));
            
            services.AddHostedService(sp => (ReadOnlyObjectStorageProvider<TEntity>) sp.GetRequiredService<IReadOnlyStorageProvider<TEntity>>());

        }

        public DtoServerBuilder<TEntity> WithSnapshot()
        {
            services.AddValidatedConfigSection<SnapshotCreationConfig>();
            services.AddSingleton<IScheduler>(_ => Scheduler.Default);
            services.AddSingleton<SnapshotCreationPlugin<TEntity>>();
            services.AddHostedService<DatasetUpdateConsumingService<TEntity, SnapshotCreationPlugin<TEntity>>>();

            return this;
        }

        public DtoServerBuilder<TEntity> WithInMemoryDataset()
        {
            services.AddSingleton<IStorageProvider<TEntity>, InMemoryStorageProvider<TEntity>>();
            services.AddSingleton<IReadOnlyStorageProvider<TEntity>>(sp => sp.GetRequiredService<IStorageProvider<TEntity>>());
            _isInMemoryDataset = true;
            return this;
        }

        public DtoServerBuilder<TEntity> WithCleanup()
        {
            services.AddValidatedConfigSection<CleanupConfig>();
            services.AddHostedService<StorageCleaningService<TEntity>>();

            return this;
        }
        
        public DtoServerBuilder<TEntity> WithDistributedLock(params string[] readinessTags)
        {
            services.AddSingleton<DistributedLockService<TEntity>>();
            services.AddHostedService(sp => sp.GetRequiredService<DistributedLockService<TEntity>>());
            services.AddHealthChecks().AddCheck<DistributedLockHealthCheck<TEntity>>($"DistributedLockHealthCheck-{typeof(TEntity).Name}", null, readinessTags);

            return this;
        }
        
        public void Build(params string[] readinessTags)
        {
            AddDefaultStorageProvider();
            services
                .AddSingleton<INotificationsHub<TEntity>, NotificationsHub<TEntity>>()
                .AddSingleton<DatasetReader<TEntity>>()
                .AddSingleton<IDatasetReader<TEntity>>(sp => sp.GetRequiredService<DatasetReader<TEntity>>());
            services.AddHostedService<DatasetReader<TEntity>>(sp => (DatasetReader<TEntity>) sp.GetRequiredService<IDatasetReader<TEntity>>());

            if (readOnlyMode)
            {
                services.AddHealthChecks()
                    .AddCheck<ReadOnlyDatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                    .AddCheck<ReadOnlyDatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
                return;

            }
           
            services.AddHealthChecks()
                .AddCheck<DatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                .AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
            services.AddValidatedConfigSection<WriterConfig>();
            services.AddSingleton<IDatasetWriter<TEntity>, DatasetWriter<TEntity>>();
            
        }


        private void AddDefaultStorageProvider()
        {
            if(_isCacheConfigured || _isInMemoryDataset)
                return;
            
            services.AddValidatedConfigSection<StorageConfig>();
            services.AddSingleton<IStorageReader<TEntity>, ObjectStorageReader<TEntity>>();
            
            if(readOnlyMode)
            {
                services.AddSingleton<IReadOnlyStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
                services.AddHostedService(sp => (ReadOnlyObjectStorageProvider<TEntity>) sp.GetRequiredService<IReadOnlyStorageProvider<TEntity>>()); 
               
                return;
            };
            
            services.AddSingleton<IStorageProvider<TEntity>, ObjectStorageProvider<TEntity>>();
            services.AddHostedService(sp => (ObjectStorageProvider<TEntity>) sp.GetRequiredService<IStorageProvider<TEntity>>());
        }
    }
}
