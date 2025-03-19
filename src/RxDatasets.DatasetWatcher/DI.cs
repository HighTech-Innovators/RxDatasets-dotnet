using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;

namespace RxDatasets.DatasetWatcher;

public class DI
{
    public static void AddDatasetWatcher<TEntity>(IServiceCollection builderServices) where TEntity : class, IMessage<TEntity>
    {
        builderServices.AddValidatedConfigSection<DatasetWatcherConfig>();
        builderServices.AddSingleton<IDatasetWatcher<TEntity>, DatasetWatcher<TEntity>>();
    }
}
