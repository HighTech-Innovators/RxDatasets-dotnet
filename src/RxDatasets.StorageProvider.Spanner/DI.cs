using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProvider.Spanner;

public static class DtoServerBuilderExtensions
{
    /// <summary>
    /// Stores the dataset in Cloud Spanner (config section <c>Spanner</c>). Do not combine with
    /// <c>WithDistributedLock()</c>, which needs blob storage: every instance can write, fenced by the version rows, and
    /// a lease row picks the one instance that runs the snapshot, cleaning and reporting plugins.
    /// </summary>
    public static DtoServerBuilder<TEntity> WithSpannerStorage<TEntity>(this DtoServerBuilder<TEntity> builder)
        where TEntity : class, IMessage<TEntity>
    {
        builder.Services.AddValidatedConfigSection<SpannerConfig>();
        builder.Services.AddSingleton<SpannerDatasetStore<TEntity>>();
        builder.Services.AddSingleton<IStorageReader<TEntity>>(sp => sp.GetRequiredService<SpannerDatasetStore<TEntity>>());
        return builder.WithStorageProvider<SpannerStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
    }
}
