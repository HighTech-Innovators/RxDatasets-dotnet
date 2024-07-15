namespace RxDatasets.Abstractions;

public interface IDatasetUpdateReporting
{
    Task OnNewDto(string Name, byte[] NewProtoBytes);
    Task OnChangedDto(string Name, byte[] NewProtoBytes);
    Task OnDeletedDto(string Name);
}