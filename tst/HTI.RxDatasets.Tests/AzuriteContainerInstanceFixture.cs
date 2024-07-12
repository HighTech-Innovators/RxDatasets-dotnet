using Testcontainers.Azurite;

namespace HTI.RxDatasets.Tests;

public class AzuriteContainerInstanceFixture : IClassFixture<AzuriteContainer>
{
    public AzuriteContainer Container { get; set; } = new AzuriteBuilder().Build();

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}