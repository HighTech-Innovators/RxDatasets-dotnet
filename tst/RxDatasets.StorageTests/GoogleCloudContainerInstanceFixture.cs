using System.Net.NetworkInformation;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;

namespace RxDatasets.StorageTests;

public class GoogleCloudContainerInstanceFixture : IClassFixture<IContainer>
{
    public IContainer Container { get; set; }
    private readonly int dynamicPort = GetRandomUnusedPort();


    public GoogleCloudContainerInstanceFixture()
    {
        string dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST") ?? "127.0.0.1";
        dockerHost = dockerHost.Replace("tcp://", "").Split(':')[0]; // Extract the host part if DOCKER_HOST is in tcp:// format

        Container = new ContainerBuilder("fsouza/fake-gcs-server:latest")
            .WithImagePullPolicy(PullPolicy.Always)
            .WithHostname(dockerHost)
            .WithPortBinding(dynamicPort) // Use the dynamic port
            .WithCommand("-scheme", "http", "-port", dynamicPort.ToString(), "-external-url", $"http://{dockerHost}:{dynamicPort}") // Use Docker host
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(dynamicPort))
            .Build();
    }

    public string GetConnectionString()
    {
        return $"http://{Container.Hostname}:{Container.GetMappedPublicPort(dynamicPort)}";
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    private static int GetRandomUnusedPort()
    {
        var random = new Random();
        int port;

        do
        {
            // Generate a random port in the allowed range (49152–65535 for dynamic/private ports)
            port = random.Next(49152, 65536);
        }
        while (IsPortInUse(port));

        return port;
    }

    private static bool IsPortInUse(int port)
    {
        var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
        var activeTcpListeners = ipGlobalProperties.GetActiveTcpListeners();

        return activeTcpListeners.Any(endpoint => endpoint.Port == port);
    }
}
