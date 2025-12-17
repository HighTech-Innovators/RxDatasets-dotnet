using System.Net.NetworkInformation;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace RxDatasets.StorageTests;

public class BigtableContainerInstanceFixture : IClassFixture<IContainer>
{
    public IContainer Container { get; set; }
    private const int BigtablePort = 8086;

    public BigtableContainerInstanceFixture()
    {
        Container = new ContainerBuilder()
            .WithImage("gcr.io/google.com/cloudsdktool/cloud-sdk:latest")
            .WithImagePullPolicy(PullPolicy.Missing)
            .WithPortBinding(BigtablePort, true)
            .WithCommand("gcloud", "beta", "emulators", "bigtable", "start", $"--host-port=0.0.0.0:{BigtablePort}")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(BigtablePort))
            .Build();
    }

    public string GetConnectionString()
    {
        return $"{Container.Hostname}:{Container.GetMappedPublicPort(BigtablePort)}";
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}
