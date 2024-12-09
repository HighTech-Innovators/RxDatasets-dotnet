namespace RxDatasets.Abstractions;

public interface IDatasetObserver<in T>
{
    Task OnDatasetVersionStart(ulong version);
    Task OnDelete(string name);
    Task OnCreate(string name, ReadOnlyMemory<byte> data);
    Task OnChange(string name, ReadOnlyMemory<byte> data);
    Task OnDatasetVersionEnd(ulong version);
}