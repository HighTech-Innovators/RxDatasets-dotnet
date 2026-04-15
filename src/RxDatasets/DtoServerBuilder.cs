using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
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
            return new DtoServerBuilder<TEntity>(services, true);
        }
    }

    public class DtoServerBuilder<TEntity>(IServiceCollection services, bool readOnlyMode = false)
        where TEntity : class, IMessage<TEntity>
    {
        private bool _withCache;
        private bool _isInMemoryDataset;
        private bool _withDistributedLock;
        private bool _withSnapshot;
        private bool _withSimpleSnapshot;
        private bool _withCleanup;
        private bool _withSharedReaderWriter;
        private string? _idPattern;
        private Action<IServiceCollection, bool, bool>? _customStorageRegistration;

        public IServiceCollection Services => services;

        public DtoServerBuilder<TEntity> WithCustomStorageRegistration(Action<IServiceCollection, bool, bool> registration)
        {
            _customStorageRegistration = registration;
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

        /// <summary>
        /// Enables a simplified snapshot creator that borrows the writer's in-memory indices
        /// and reads entity data via the multi-source read API (random-access, cache-friendly).
        /// Does not store version history — suitable for deployments that don't need
        /// <c>ListChanges</c> for ranges beyond what is still available in delta files.
        /// Mutually exclusive with <see cref="WithSnapshot"/>.
        /// </summary>
        public DtoServerBuilder<TEntity> WithSimpleSnapshot()
        {
            _withSimpleSnapshot = true;
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

        /// <summary>
        /// Uses a single combined reader/writer instance that shares one index and one
        /// initialization path, halving storage round-trips at startup.
        /// Best for datasets with infrequent writes; high-write workloads should use the
        /// default separate reader/writer to preserve incremental parent-index updates.
        /// </summary>
        public DtoServerBuilder<TEntity> WithSharedReaderWriter()
        {
            _withSharedReaderWriter = true;
            return this;
        }

        /// <summary>
        /// Configures a fixed ID pattern for entity names (e.g. "t/*/s/*/items/*/values").
        /// Fixed segments (non-*) are stripped from the in-memory trie, reducing node count
        /// and memory by up to 50% for deeply nested hierarchical entity names.
        /// The on-disk format is not affected.
        /// </summary>
        public DtoServerBuilder<TEntity> WithIdPattern(string pattern)
        {
            _idPattern = pattern;
            return this;
        }

        /// <summary>
        /// Disables write coalescing. By default, concurrent write operations are batched into single
        /// delta transactions to reduce storage round-trips on high-latency backends.
        /// Call this method to revert to serial one-at-a-time write behavior.
        /// </summary>
        public DtoServerBuilder<TEntity> WithoutWriteCoalescing()
        {
            services.PostConfigure<WriterConfig>(c => c.EnableWriteCoalescing = false);
            return this;
        }

        public void Build(params string[] readinessTags)
        {
            services.TryAddSingleton<IClock, SystemClock>();

            // Register NameCompressor if an ID pattern is configured
            var compressor = NameCompressor.FromPattern(_idPattern);
            if (compressor != null)
            {
                services.AddSingleton(compressor);
            }

            AddStorageProvider();

            if (!_isInMemoryDataset && _withDistributedLock)
            {
                services.AddValidatedConfigSection<DistributedLockOptions>("DistributedLock");
                services.TryAddSingleton<ILockProvider, BlobStorageLockProvider<TEntity>>();
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

            if (!_isInMemoryDataset && _withSimpleSnapshot)
            {
                services.AddValidatedConfigSection<SnapshotCreationConfig>();
                services.TryAddSingleton<IScheduler>(_ => Scheduler.Default);
                services.AddSingleton<SimpleSnapshotCreationPlugin<TEntity>>();
                services.AddHostedService<DatasetUpdateConsumingService<TEntity, SimpleSnapshotCreationPlugin<TEntity>>>();
            }

            if (!_isInMemoryDataset && _withCleanup)
            {
                services.AddValidatedConfigSection<CleanupConfig>();
                services.AddHostedService<StorageCleaningService<TEntity>>();
            }

            services.AddSingleton<INotificationsHub<TEntity>, NotificationsHub<TEntity>>();

            if (readOnlyMode)
            {
                services.AddSingletonAsHostedService<IDatasetReader<TEntity>, DatasetReader<TEntity>>();
                services.AddHealthChecks()
                    .AddCheck<ReadOnlyDatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                    .AddCheck<ReadOnlyDatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
                return;
            }

            services.AddHealthChecks()
                .AddCheck<DatasetReadinessCheck<TEntity>>($"DatasetReadinessCheck-{typeof(TEntity).Name}", null, readinessTags)
                .AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");

            services.AddValidatedConfigSection<WriterConfig>();

            if (_withSharedReaderWriter)
            {
                services.AddSingleton<DatasetReaderWriter<TEntity>>();
                services.AddSingletonAlias<IDatasetReader<TEntity>, DatasetReaderWriter<TEntity>>();
                services.AddSingletonAlias<IDatasetWriter<TEntity>, DatasetReaderWriter<TEntity>>();
                services.AddSingletonAlias<ICurrentIndexProvider<TEntity>, DatasetReaderWriter<TEntity>>();
            }
            else
            {
                services.AddSingleton<IDatasetReader<TEntity>, DatasetReader<TEntity>>();
                services.AddSingleton<IDatasetWriter<TEntity>, DatasetWriter<TEntity>>();
                services.AddSingleton<ICurrentIndexProvider<TEntity>>(sp => (ICurrentIndexProvider<TEntity>)sp.GetRequiredService<IDatasetWriter<TEntity>>());
            }

            if (!_isInMemoryDataset)
            {
                services.AddSingleton<DataMigrator<TEntity>>();
                services.AddSingleton<IDataMigrator>(sp => sp.GetRequiredService<DataMigrator<TEntity>>());
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

            if (_customStorageRegistration != null)
            {
                _customStorageRegistration(services, readOnlyMode, _withCache);
                return;
            }

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
