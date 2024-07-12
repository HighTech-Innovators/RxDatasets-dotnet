using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using Testcontainers.Minio;

namespace HTI.RxDatasets.Tests
{
    public class MinioTestContainerFixture : IClassFixture<MinioContainer>
    {
        public MinioContainer Container { get; set; }
        public INetwork Network { get; set; }
        public MinioTestContainerFixture()
        {
            Network = new NetworkBuilder()
                .WithName(Guid.NewGuid().ToString("D"))
                .Build();
            Network.CreateAsync().Wait();
            Container = new MinioBuilder().WithNetwork(Network).WithNetworkAliases("minio").Build();
        }
        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }
    }
}