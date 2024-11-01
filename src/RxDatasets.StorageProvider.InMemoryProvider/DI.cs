using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.InMemory
{
    public static class DtoServerBuilderExtensions
    {
        public static DtoServerBuilder<TEntity> WithInMemoryStorageProvider<TEntity>(this DtoServerBuilder<TEntity> builder) where TEntity : class, IMessage<TEntity>
        {
            builder.Services.AddSingleton<IObjectStorageProvider<TEntity>, LowLevelInMemoryStorageProvider<TEntity>>();
            builder.Services.AddHealthChecks().AddCheck<InMemoryStorageHealthCheck<TEntity>>($"storage.inMemory.{typeof(TEntity).Name}");

            return builder;
        }
    }
}