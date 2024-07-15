using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.Models.Config;
using RxDatasets.StorageProviders;

namespace RxDatasets;

public static class DI
{
    public static void ConfigureServices<TEntity>(IServiceCollection builderServices) where TEntity : class, IMessage<TEntity>
    {
        builderServices.AddSingleton(serviceProvider => serviceProvider);
        builderServices.AddHealthChecks().AddCheck<DatasetHealthCheck<TEntity>>($"DatasetHealthCheck-{typeof(TEntity).Name}");

    }

    private static void AddNonKeyedInjects(IServiceCollection builderServices)
    {

        builderServices.AddValidatedConfigSection<CleanupConfig>();
        builderServices.AddValidatedConfigSection<SnapshotCreationConfig>();
        builderServices.AddSingleton<IScheduler>(_ => Scheduler.Default);
        builderServices.AddValidatedConfigSection<DtoConfig>();
    }

    private static void AddSingletonsForDataset<TEntity>(this IServiceCollection services) where TEntity : class, IMessage<TEntity>
    {
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
        AddNonKeyedInjects(services);
        services.AddHostedService<StorageCleaningService<TEntity>>();
        services.AddHostedService<SnapshotCreationService<TEntity>>();
        services.AddHostedService<DatasetUpdateReportingService<TEntity>>();

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
        AddNonKeyedInjects(services);
        AddSingletonsForDataset<TEntity>(services);
        services.AddHostedService<SnapshotCreationService<TEntity>>();
        services.AddHostedService<StorageCleaningService<TEntity>>();
        services.AddHostedService<DatasetUpdateReportingService<TEntity>>();

    }

}