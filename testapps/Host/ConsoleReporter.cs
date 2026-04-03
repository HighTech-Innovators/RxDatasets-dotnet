using RxDatasets.Abstractions;

namespace Host
{
    internal class ConsoleReporter<T> : IDatasetUpdateReporting<T>
    {
        public Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction)
        {
            Console.WriteLine(name + " was added with new bytes length: " + newProtoBytes.Length);
            return Task.CompletedTask;
        }

        public Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction)
        {
            Console.WriteLine(name + " was updated with new bytes length: " + newProtoBytes.Length);
            return Task.CompletedTask;
        }

        public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction)
        {
            Console.WriteLine(name + " was deleted");
            return Task.CompletedTask;
        }

        public string ReportingIdentifier { get; } = $"ConsoleReporter-for-{typeof(T).Name}";
    }
}
