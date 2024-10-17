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
    }

    public class DtoServerBuilder<TEntity>(IServiceCollection services)
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

            services.AddValidatedConfigSection<DtoStorageCacheConfig>();
            services.AddSingleton<ObjectStorageProvider<TEntity>>();
            services.AddHostedService(sp => sp.GetRequiredService<ObjectStorageProvider<TEntity>>());
            services.AddSingleton<IStorageProvider<TEntity>>(sp =>
                new CacheStorageProvider<TEntity>(
                    sp.GetRequiredService<ObjectStorageProvider<TEntity>>(),
                    sp.GetRequiredService<IOptions<DtoStorageCacheConfig>>(),
                    sp.GetRequiredService<ILogger<CacheStorageProvider<TEntity>>>()
                ));

            services.AddHostedService(sp => (CacheStorageProvider<TEntity>) sp.GetRequiredService<IStorageProvider<TEntity>>());
            _isCacheConfigured = true;

            return this;
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
            _isInMemoryDataset = true;
            return this;
        }

        public DtoServerBuilder<TEntity> WithCleanup()
        {
            services.AddValidatedConfigSection<CleanupConfig>();
            services.AddHostedService<StorageCleaningService<TEntity>>();

            return this;
        }

        public DtoServerBuilder<TEntity> WithDistributedLock(params string[] healthTags)
        {
            services.AddSingleton<DistributedLockService<TEntity>>();
            services.AddHostedService(sp => sp.GetRequiredService<DistributedLockService<TEntity>>());
            services.AddHealthChecks().AddCheck<DistributedLockHealthCheck<TEntity>>($"DistributedLockHealthCheck-{typeof(TEntity).Name}", null, healthTags);

            return this;
        }
        
        public IServiceCollection Build()
        {
            AddDefaultStorageProvider();
            services.AddHealthChecks().AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
            services.AddValidatedConfigSection<StorageConfig>();
            services.AddValidatedConfigSection<WriterConfig>();

            services.AddSingleton<INotificationsHub<TEntity>, NotificationsHub<TEntity>>()
                     .AddSingleton<IDatasetReader<TEntity>, DatasetReader<TEntity>>()
                     .AddSingleton<IDatasetWriter<TEntity>, DatasetWriter<TEntity>>();

            return services;
        }

        private void AddDefaultStorageProvider()
        {
            if(_isCacheConfigured || _isInMemoryDataset)
                return;

            services.AddSingleton<IStorageProvider<TEntity>, ObjectStorageProvider<TEntity>>();
            services.AddHostedService(sp => (ObjectStorageProvider<TEntity>) sp.GetRequiredService<IStorageProvider<TEntity>>());
         
        }
    }
}
