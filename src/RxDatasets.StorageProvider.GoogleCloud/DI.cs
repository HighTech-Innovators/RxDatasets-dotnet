using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RxDatasets.Abstractions;
using RxDatasets.StorageProviders.ReadOnly;

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

        /// <summary>
        /// Stores the dataset in Bigtable (<see cref="GoogleCloudConfig.BigtableInstanceId"/>,
        /// <see cref="GoogleCloudConfig.BigtableTableId"/>), with payloads too large for a row, the lock and the processed
        /// versions in the bucket.
        /// </summary>
        public static DtoServerBuilder<TEntity> WithGoogleBigtableStorage<TEntity>(this DtoServerBuilder<TEntity> builder)
            where TEntity : class, IMessage<TEntity>
        {
            builder.WithGoogleCloudStorage();
            builder.Services.TryAddSingleton<BigtableRowStore>();
            builder.Services.AddSingleton<IStorageReader<TEntity>, BigtableStorageReader<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<BigtableHealthCheck>($"storage.bigtable.{typeof(TEntity).Name}",
                tags: ["bigtable", typeof(TEntity).Name]);
            return builder.WithStorageProvider<BigtableStorageProvider<TEntity>, ReadOnlyObjectStorageProvider<TEntity>>();
        }
    }
}
