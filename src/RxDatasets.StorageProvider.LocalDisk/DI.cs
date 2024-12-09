using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.LocalDisk
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithLocalDiskStorage<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddValidatedConfigSection<LocalDiskStorageConfig>();
            builder.Services.AddSingleton<IDiskStorage<TEntity>, LocalDiskStorage<TEntity>>();
            return builder;
        }
    }
}