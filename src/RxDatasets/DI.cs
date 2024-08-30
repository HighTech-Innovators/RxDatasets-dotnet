using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.StorageProviders;

namespace RxDatasets;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices) where TEntity : class, IMessage<TEntity>
    {
        builderServices.AddSingleton(serviceProvider => serviceProvider);
        builderServices.AddHealthChecks().AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");
    }

    private static void AddSingletonsForDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        services.AddValidatedConfigSection<StorageConfig>();

        services
            .AddSingleton<IDatasetUpdateNotifier<TEntity>, DatasetUpdateNotifierAsync<TEntity>>()
            .AddSingleton<IDatasetReader<TEntity>, DatasetReader<TEntity>>()
            .AddHostedService(sp => (DatasetReader<TEntity>) sp.GetRequiredService<IDatasetReader<TEntity>>())
            .AddSingleton<IDatasetWriter<TEntity>, DatasetWriter<TEntity>>()
            .AddHostedService(sp => (DatasetWriter<TEntity>)sp.GetRequiredService<IDatasetWriter<TEntity>>());

    }


    public static void AddInMemorySingletonDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        services.AddSingleton<IStorageProvider<TEntity>, InMemoryStorageProvider<TEntity>>();
        AddSingletonsForDataset<TEntity>(services);
        AddExtraCustomServices<TEntity>(services);
    }

    private static void AddExtraCustomServices<TEntity>(IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
        AddStorageCleaningDependencies<TEntity>(services);
        AddSnapshotCreationDependencies<TEntity>(services);
    }

    private static void AddStorageCleaningDependencies<TEntity>(IServiceCollection services)
        where TEntity : class, IMessage<TEntity>
    {
        services.AddValidatedConfigSection<CleanupConfig>();
        services.AddHostedService<StorageCleaningService<TEntity>>();
    }

    private static void AddSnapshotCreationDependencies<TEntity>(IServiceCollection services)
        where TEntity : class, IMessage<TEntity>
    {
        services.AddValidatedConfigSection<SnapshotCreationConfig>();
        services.AddSingleton<IScheduler>(_ => Scheduler.Default);
        // Register the plugins as singleton services
        services.AddSingleton<SnapshotCreationPlugin<TEntity>>();
        // Register the hosted services using the concrete types of TConsumer
        services.AddHostedService<DatasetUpdateConsumingService<TEntity, SnapshotCreationPlugin<TEntity>>>();
    }

    public static void AddCachedObjectStorageDataset<TEntity>(this IServiceCollection services) where TEntity: class, IMessage<TEntity>
    {
        services.AddValidatedConfigSection<DtoStorageCacheConfig>();
        services.AddSingleton<ObjectStorageProvider<TEntity>>();
        services.AddHostedService(sp => sp.GetRequiredService<ObjectStorageProvider<TEntity>>());
        services.AddSingleton<IStorageProvider<TEntity>>( (sp) => 
            new CacheStorageProvider<TEntity>(
                sp.GetRequiredService<ObjectStorageProvider<TEntity>>(),
                sp.GetRequiredService<IOptions<DtoStorageCacheConfig>>(),
                sp.GetRequiredService<ILogger<CacheStorageProvider<TEntity>>>()
                )
        );
        AddSingletonsForDataset<TEntity>(services);
        AddExtraCustomServices<TEntity>(services);
    }

}