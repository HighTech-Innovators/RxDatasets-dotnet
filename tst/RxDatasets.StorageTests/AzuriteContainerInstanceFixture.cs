using Testcontainers.Azurite;

namespace RxDatasets.StorageTests;

public class AzuriteContainerInstanceFixture : IClassFixture<AzuriteContainer>
{
    public AzuriteContainer Container { get; set; } = new AzuriteBuilder().Build();

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}