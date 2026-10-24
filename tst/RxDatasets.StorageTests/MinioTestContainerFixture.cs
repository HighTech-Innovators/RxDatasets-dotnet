using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;
using DotNet.Testcontainers.Networks;
using Testcontainers.Minio;

namespace RxDatasets.StorageTests
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
            Container = new MinioBuilder()
                .WithImage("pgsty/minio:latest")
                .WithImagePullPolicy(PullPolicy.Always)
                .WithNetwork(Network)
                .WithNetworkAliases("minio")
                .Build();
        }
        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }
    }
}
