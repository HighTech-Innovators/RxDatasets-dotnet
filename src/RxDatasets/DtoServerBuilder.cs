using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
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
            return new DtoServerBuilder<TEntity>(services).WithoutWriter();
        }

        public static DtoServerBuilder<TEntity> AddWriteOnlyDtoServer<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
        {
            services.AddSingleton(serviceProvider => serviceProvider);
            return new DtoServerBuilder<TEntity>(services).WithoutReader();
        }
    }

    public class DtoServerBuilder<TEntity>(IServiceCollection services)
        where TEntity : class, IMessage<TEntity>
    {
        private bool _withCache;
        private bool _isInMemoryDataset;
        private bool _withDistributedLock;
        private bool _withSnapshot;
        private bool _withCleanup;
        private bool _withReader = true;
        private bool _withWriter = true;
        private bool readOnlyMode => _withReader && !_withWriter && !_withCleanup && !_withSnapshot && !_withDistributedLock;

        public IServiceCollection Services => services;

        public DtoServerBuilder<TEntity> WithoutReader()
        {
            _withReader = false;
            return this;
        }

        public DtoServerBuilder<TEntity> WithoutWriter()
        {
            _withWriter = false;
            return this;
        }

        public DtoServerBuilder<TEntity> WithCache()
        {
            _withCache = true;
            return this;
        }

        private void AddReadOnlyObjectStorage()
        {
            // Register the 'actual' ObjectStorageProvider as HostedService
            services.AddSingletonAsHostedService<ReadOnlyObjectStorageProvider<TEntity>>();

            if (_withCache)
            {
                services.AddValidatedConfigSection<DtoStorageCacheConfig>();
                services.AddSingletonAsHostedService<IReadOnlyStorageProvider<TEntity>,
                        ReadOnlyCacheStorageProvider<TEntity,
                            ReadOnlyObjectStorageProvider<TEntity>>>(); // Register the cache
            }
            else
            {
                services.AddSingletonAlias<IReadOnlyStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
            }
        }

        private void AddReadWriteObjectStorage()
        {
            // Register the 'actual' ObjectStorageProvider as HostedService
            services.AddSingletonAsHostedService<ObjectStorageProvider<TEntity>>();

            // Register the cache as the 'to be used' provider (for R/W, and R/O), and register that as Hosted Service
            if (_withCache)
            {
                services.AddValidatedConfigSection<DtoStorageCacheConfig>();
                services.AddSingletonAsHostedService<IStorageProvider<TEntity>,
                    CacheStorageProvider<TEntity, ObjectStorageProvider<TEntity>>>(); // Register the cache
            }
            else
            {
                services.AddSingletonAlias<IStorageProvider<TEntity>, ObjectStorageProvider<TEntity>>();
            }

            services.AddSingletonAlias<IReadOnlyStorageProvider<TEntity>, IStorageProvider<TEntity>>();
        }

        public DtoServerBuilder<TEntity> WithSnapshot()
        {
            _withSnapshot = true;
            return this;
        }

        public DtoServerBuilder<TEntity> WithInMemoryDataset()
        {
            _isInMemoryDataset = true;
            return this;
        }

        public DtoServerBuilder<TEntity> WithCleanup()
        {
            _withCleanup = true;
            return this;
        }

        public DtoServerBuilder<TEntity> WithDistributedLock()
        {
            _withDistributedLock = true;
            return this;
        }

        public void Build(params string[] readinessTags)
        {
            AddStorageProvider();

            if (!_isInMemoryDataset && _withDistributedLock)
            {
                services.AddSingletonAsHostedService<DistributedLockService<TEntity>>();
                services.AddHealthChecks().AddCheck<DistributedLockHealthCheck<TEntity>>($"DistributedLockHealthCheck-{typeof(TEntity).Name}", null, readinessTags);
            }

            if (!_isInMemoryDataset && _withSnapshot)
            {
                services.AddValidatedConfigSection<SnapshotCreationConfig>();
                services.AddSingleton<IScheduler>(_ => Scheduler.Default);
                services.AddSingleton<SnapshotCreationPlugin<TEntity>>();
                services.AddHostedService<DatasetUpdateConsumingService<TEntity, SnapshotCreationPlugin<TEntity>>>();
            }

            if (!_isInMemoryDataset && _withCleanup)
            {
                services.AddValidatedConfigSection<CleanupConfig>();
                services.AddHostedService<StorageCleaningService<TEntity>>();
            }

            services.AddSingleton<INotificationsHub<TEntity>, NotificationsHub<TEntity>>();

            if (_withReader)
            {
                if (!readOnlyMode)
                    services.AddSingleton<IDatasetReader<TEntity>, DatasetReader<TEntity>>();
                else
                    services.AddSingletonAsHostedService<IDatasetReader<TEntity>, DatasetReader<TEntity>>();
            }

            if (readOnlyMode)
            {
                services.AddHealthChecks()
                    .AddCheck<ReadOnlyDatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                    .AddCheck<ReadOnlyDatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
                return;
            }

            if (_withWriter)
            {
                services.AddHealthChecks()
                    .AddCheck<DatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                    .AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");

                services.AddValidatedConfigSection<WriterConfig>();
                services.AddSingleton<IDatasetWriter<TEntity>, DatasetWriter<TEntity>>();
            }
        }

        private void AddStorageProvider()
        {
            if (_isInMemoryDataset)
            {
                services.AddSingleton<IStorageProvider<TEntity>, InMemoryStorageProvider<TEntity>>();
                services.AddSingletonAlias<IReadOnlyStorageProvider<TEntity>, IStorageProvider<TEntity>>();
                return;
            }

            // Register the usage of ObjectStorage
            services.AddValidatedConfigSection<StorageConfig>();
            services.AddSingleton<IStorageReader<TEntity>, ObjectStorageReader<TEntity>>();

            if (readOnlyMode)
                AddReadOnlyObjectStorage();
            else
                AddReadWriteObjectStorage();
        }
    }

    public static class DependencyInjectionExtensions
    {
        public static IServiceCollection AddSingletonAlias<TInterface, TImplementation>(this IServiceCollection sc)
            where TImplementation : TInterface
            where TInterface : class
            => sc.AddSingleton<TInterface>(sp => sp.GetRequiredService<TImplementation>());

        public static IServiceCollection AddSingletonAsHostedService<TService>(
            this IServiceCollection services)
            where TService : class, IHostedService
        {
            services.AddSingleton<TService>();
            services.AddHostedService(sp => sp.GetRequiredService<TService>());
            return services;
        }

        public static IServiceCollection AddSingletonAsHostedService<TInterface, TService>(
            this IServiceCollection services)
            where TService : class, IHostedService, TInterface
            where TInterface : class
        {
            services.AddSingleton<TService>();
            services.AddHostedService(sp => sp.GetRequiredService<TService>());
            services.AddSingletonAlias<TInterface, TService>();
            return services;
        }

    }
}
