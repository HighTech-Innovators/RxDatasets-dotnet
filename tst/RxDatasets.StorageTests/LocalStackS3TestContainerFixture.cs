using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using System.Net;

namespace RxDatasets.StorageTests;

public class LocalStackS3TestContainerFixture : IAsyncLifetime
{
    public IContainer Container { get; }
    public INetwork Network { get; }
    public string AccessKey => "test"; // LocalStack default
    public string SecretKey => "test"; // LocalStack default

    public LocalStackS3TestContainerFixture()
    {
        Network = new NetworkBuilder()
            .WithName(Guid.NewGuid().ToString("D"))
            .Build();
        Network.CreateAsync().Wait();

        Container = new ContainerBuilder()
            .WithImage("localstack/localstack:s3-latest")
            .WithNetwork(Network)
            .WithNetworkAliases("localstack")
            .WithEnvironment("SERVICES", "s3")
            .WithPortBinding(4566, true) // edge port
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(request => request
                    .ForPort(4566)
                    .ForPath("/_localstack/health")
                    .ForStatusCode(HttpStatusCode.OK))
            )
            .Build();
    }

    public async Task InitializeAsync()
    {
        if (Container.State != TestcontainersStates.Running)
        {
            await Container.StartAsync();
        }
    }

    public async Task DisposeAsync()
    {
        if (Container.State == TestcontainersStates.Running)
        {
            await Container.StopAsync();
        }
    }
}
