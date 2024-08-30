namespace RxDatasets.Abstractions;

public interface IDatasetUpdateConsumer<T> 
{
    Task HandleNewDeltas(CancellationToken stoppingToken);

    string ConsumerIdentifier { get; }
}