using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Runtime.CompilerServices;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;
using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel.Args;
using RxDatasets.Configs;
using RxDatasets.Models;
using RxDatasets.StorageProviders;
using RxDatasets.StorageProvider.Minio;

namespace RxDatasets.StorageTests
{
    public class E2ETestsUsingRealHost(
        MinioTestContainerFixture minioContainer,
        DemodtoHostContainerFixture hostContainer)
        : IClassFixture<MinioTestContainerFixture>, IClassFixture<DemodtoHostContainerFixture>, IAsyncLifetime
    {
        private Uri? _healthCheckUri;
        private Uri? _appUri;
        private IMinioClient? _minioClient;
        private string _bucketName = "";
        private static readonly List<string> SevenUniqueNames =
            ["unique_name1", "unique_name2", "unique_name3", "unique_name4", "unique_name5", "unique_name6", "unique_name7"];

        private static readonly List<string> SevenStrings =
            ["deserunt1", "deserunt2", "deserunt3", "deserunt4", "deserunt5", "deserunt6", "deserunt7"];

        private IOptions<MinioS3Config>? _s3Config;


        public async Task InitializeAsync()
        {

            if (minioContainer.Container.State != TestcontainersStates.Running)
                await minioContainer.Container.StartAsync();

            _s3Config = Options.Create(new MinioS3Config
            {
                AccessKey = minioContainer.Container.GetAccessKey(),
                SecretKey = minioContainer.Container.GetSecretKey(),
                ServiceURL = $"{minioContainer.Container.IpAddress}:{9000}"
            });

            _minioClient = new MinioClient().WithEndpoint($"{minioContainer.Container.Hostname}:{minioContainer.Container.GetMappedPublicPort(9000)}")
                .WithCredentials(_s3Config.Value.AccessKey, _s3Config.Value.SecretKey)
                .Build();
        }

        private async Task StartHostContainer([CallerMemberName] string memberName = "")
        {
            _bucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
            await hostContainer.StartContainer(_s3Config?.Value, minioContainer.Network, _bucketName);
            _healthCheckUri = new UriBuilder("http", hostContainer.DemoDtoHostContainerInstance?.Hostname,
                hostContainer.DemoDtoHostContainerInstance!.GetMappedPublicPort(18080), "/healthz").Uri;
            _appUri = new UriBuilder("http", hostContainer.DemoDtoHostContainerInstance.Hostname,
                hostContainer.DemoDtoHostContainerInstance.GetMappedPublicPort(10080)).Uri;
        }

        public async Task DisposeAsync()
        {
            await hostContainer.DisposeAsync();
            await minioContainer.DisposeAsync();
        }


        [Fact]
        public async Task Test_Service_Started_And_Healthy()
        {
            await StartHostContainer();
            var httpClient = new HttpClient();
            var response = await httpClient.GetAsync(_healthCheckUri);
            var content = await response.Content.ReadAsStringAsync();
            content.Should().Be("Healthy");
        }

        [Fact]
        public async Task Read_Raw_Snapshot_Data_Tests()
        {

            await StartHostContainer();
            using var channel = GrpcChannel.ForAddress(_appUri!);
            var writerClient = new WriterService.WriterServiceClient(channel);
            MessageParser<Demodto> protoParser = new(() => (Demodto)Activator.CreateInstance(typeof(Demodto))!);

            // Load base payload
            var basePayload = new Demodto();
            // Perform initial gRPC calls with unique names and some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], SevenStrings[i]);
                var response = writerClient.Put(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            var storageConfig = new StorageConfig { CustomRootFolder = "customRootFolder/" };

            ulong latestSnapshotVersion;
            while (true)
            {
                var listOfSnapshots = await _minioClient!.ListObjectsAsync(new ListObjectsArgs()
                        .WithBucket(_bucketName)
                        .WithPrefix(ObjectStorageProvider<Demodto>.GetObjectNamePrefix(storageConfig) + "snapshot/"))
                    .ToList()
                    .ToTask();

                if (listOfSnapshots.Any())
                {
                    latestSnapshotVersion = (ulong)listOfSnapshots.OrderBy(d => SharedMethods.ExtractNumber(d.Key))
                        .GroupBy(g => SharedMethods.ExtractNumber(g.Key)).Max(gr => gr.Key);

                    if (latestSnapshotVersion > 1)
                        break;
                }

                await Task.Delay(10);
            }

            await hostContainer.DemoDtoHostContainerInstance?.StopAsync()!;
            using var resultStream = new MemoryStream();
            await _minioClient?.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(ObjectStorageProvider<Demodto>.GetObjectName(latestSnapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig))
                .WithCallbackStream(x => x.CopyTo(resultStream))
            )!;

            resultStream.Position = 0;

            //verify
            var entities = new List<Demodto>();

            // Continue reading until end of stream
            while (resultStream.Position < resultStream.Length)
            {

                var demodto = protoParser.ParseDelimitedFrom(resultStream);
                if (demodto != null)
                {
                    entities.Add(demodto);
                }
            }

            // Since we only add objects, version X should have X entities
            entities.Should().HaveCount((int)latestSnapshotVersion);

            // Snapshot entity order is undefined, so order them by name, and we should have the entities as we wrote them.
            var orderedEnumerable = entities.OrderBy(e => e.Name).ToList();

            for (var index = 0; index < orderedEnumerable.Count; index++)
            {
                var entity = orderedEnumerable[index];
                entity.Name.Should().Be(SevenUniqueNames[index]);
            }
        }


        [Fact]
        public async Task Do_Generic_Write_And_Read_Tests()
        {

            await StartHostContainer();
            using var channel = GrpcChannel.ForAddress(_appUri!);
            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Load base payload
            var basePayload = new Demodto();

            // Perform initial gRPC calls with unique names and some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], SevenStrings[i]);
                var response = writerClient.Put(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            // Perform update gRPC calls with modified some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var newString = $"updated_{SevenStrings[i]}";
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], newString);
                var response = writerClient.Put(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 0, 1).Should().BeTrue();
            }

            // So now we have dataset versions 1..14
            // We expect a snaphot 1, 6, 11 - but can't guarantee that because of the async nature. But at least one should appear at some point, if all works.
            var fetchSnapshots = Task.Run(async () =>
            {
                while (true)
                {
                    var observable = _minioClient?.ListObjectsAsync(new ListObjectsArgs()
                        .WithBucket(_bucketName)
                        .WithPrefix("customRootFolder/Demodto/snapshot/")
                        .WithRecursive(true));

                    var snapshots = (await observable!.Select(s => s.Key).ToList().ToTask()).GroupBy(SharedMethods.ExtractNumber).Select(g => g.Key).ToList();

                    if (snapshots.Any())
                        return snapshots.Count;

                    // Prevent an absolute racing thread
                    await Task.Delay(10);
                }
            });
            var snapshotCount = await fetchSnapshots.WaitAsync(TimeSpan.FromSeconds(5));
            snapshotCount.Should().BeInRange(1, 3, "Ideally we get 1, 6, 11; but also valid is just 14 on the other end of the spectrum. More than 3 is never good as the interval should be 5 at least");
            // action 
            await hostContainer.DemoDtoHostContainerInstance?.StopAsync()!;

            // assert
            var action = async (int i = 0) => await readerClient.GetDemodtoAsync(new GetDemodtoRequest { Name = SevenUniqueNames[i] });
            Func<Task> actionWrapper = async () => await action();
            await actionWrapper.Should().ThrowAsync<RpcException>();

            //more action
            await hostContainer.DemoDtoHostContainerInstance?.StartAsync()!;

            _appUri = new UriBuilder("http", hostContainer.DemoDtoHostContainerInstance.Hostname,
                hostContainer.DemoDtoHostContainerInstance.GetMappedPublicPort(10080)).Uri;
            using var newChannel = GrpcChannel.ForAddress(_appUri!);
            readerClient = new ReaderService.ReaderServiceClient(newChannel);
            // more assert
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var responseDto = await action(i);
                responseDto.SomeString.Should().Be($"updated_{SevenStrings[i]}");
            }
        }
        private static Demodto UpdatePayload(Demodto basePayload, string newName, string newString)
        {
            basePayload.Name = newName;
            basePayload.SomeString = newString;
            return basePayload;
        }

        private static bool VerifyResponseIsExpected(WriteResponse response, uint expectedAdded, uint expectedUpdated)
        {
            var numAdded = response.NumAddedEntities;
            var numUpdated = response.NumUpdatedEntities;

            return numAdded == expectedAdded && numUpdated == expectedUpdated;
        }
    }
}
