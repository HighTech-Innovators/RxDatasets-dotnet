using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProvider.Spanner;

public static class DtoServerBuilderExtensions
{
    /// <summary>
    /// Stores the dataset in Cloud Spanner (config section <c>Spanner</c>). Do not combine with
    /// <c>WithDistributedLock()</c>: writers are fenced by the version rows instead, and the lock needs blob storage.
    /// </summary>
    public static DtoServerBuilder<TEntity> WithSpannerStorage<TEntity>(this DtoServerBuilder<TEntity> builder)
        where TEntity : class, IMessage<TEntity>
    {
        builder.Services.AddValidatedConfigSection<SpannerConfig>();
        builder.Services.TryAddSingleton<SpannerDatasetStore<TEntity>>();
        builder.Services.TryAddSingleton<IStorageReader<TEntity>>(sp => sp.GetRequiredService<SpannerDatasetStore<TEntity>>());
        return builder.WithStorageProvider<SpannerStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
    }
}
