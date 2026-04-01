namespace RxDatasets.Abstractions;

public interface IDatasetUpdateConsumer<T>
{
    Task HandleNewDeltas(CancellationToken stoppingToken);

    string ConsumerIdentifier { get; }

    TimeSpan StartupDelay => TimeSpan.Zero;
}
