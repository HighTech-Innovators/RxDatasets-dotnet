using RxDatasets.Abstractions;

namespace Host
{
    internal class ConsoleReporter : IDatasetUpdateReporting
    {
        public Task OnChangedDto(string Name, byte[] NewProtoBytes)
        {
            Console.WriteLine(Name +" was updated with new bytes length: "+ NewProtoBytes.Length);
            return Task.CompletedTask;
        }

        public Task OnDeletedDto(string Name)
        {
            Console.WriteLine(Name + " was deleted");
            return Task.CompletedTask;
        }

        public Task OnNewDto(string Name, byte[] NewProtoBytes)
        {
            Console.WriteLine(Name + " was added with new bytes length: " + NewProtoBytes.Length);
            return Task.CompletedTask;
        }
    }
}