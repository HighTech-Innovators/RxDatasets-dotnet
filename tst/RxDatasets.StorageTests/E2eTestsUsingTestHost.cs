using System.Runtime.CompilerServices;
using FluentAssertions;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Mvc.Testing;
using Grpc.Net.Client.Web;
using Moq;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests
{
    public class E2ETestsUsingTestHost(
        GrpcWebApplicationFactory factory)
        : IClassFixture<GrpcWebApplicationFactory>, IAsyncLifetime
    {
        private string _customPrefix = "";
        private static readonly List<string> SevenUniqueNames =
            ["unique_name1", "unique_name2", "unique_name3", "unique_name4", "unique_name5", "unique_name6", "unique_name7"];

        private static readonly List<string> SevenStrings =
            ["deserunt1", "deserunt2", "deserunt3", "deserunt4", "deserunt5", "deserunt6", "deserunt7"];


        public Task InitializeAsync()
        {
            Environment.SetEnvironmentVariable("SnapshotCreation__SnapshotInterval", "5");
            return Task.CompletedTask;
        }

        private async Task<(WebApplicationFactory<Program> factory, HttpClient healthClient, GrpcChannel channel)> CreateWebhostFactoryAsync(IObjectStorageProvider<Demodto> storageProvider, bool waitForHealthy = true,  GrpcWebApplicationFactory? factoryInput = null, [CallerMemberName] string memberName = "")
        {
            factoryInput ??= new GrpcWebApplicationFactory();
            _customPrefix = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

            // Create a new factory with custom configuration
            var customFactory = factoryInput.WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, config) =>
                {
                    config.AddInMemoryCollection(new Dictionary<string, string?>
                    {
                        { "Storage:CustomRootFolder", _customPrefix + "/customRootFolder/" },
                    });
                });

                builder.ConfigureTestServices(services =>
                {
                   services.AddSingleton(storageProvider);
                });
            });

            var healthClient = customFactory.CreateClient();
            var grpcWebHandler = new GrpcWebHandler(GrpcWebMode.GrpcWebText, customFactory.Server.CreateHandler());
            var httpClient = customFactory.CreateDefaultClient(grpcWebHandler);
            var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions { HttpClient = httpClient });

            var counter = 0;
            var healthResponse = await healthClient.GetAsync("/healthz");
            string healthContent = await healthResponse.Content.ReadAsStringAsync();

            while (waitForHealthy && healthContent != "Healthy" && counter < 100)
            {
                healthResponse = await healthClient.GetAsync("/healthz");
                healthContent = await healthResponse.Content.ReadAsStringAsync();

                await Task.Delay(TimeSpan.FromSeconds(0.5));
                counter++;
            }

            if (waitForHealthy && healthContent != "Healthy")
                throw new InvalidOperationException("Waiting for healthy status is enabled and we are not healthy, sorry, you can set the method not to wait :) ");

            return (customFactory, healthClient, channel);
        }

        public async Task DisposeAsync()
        {
            await factory.DisposeAsync();
        }

        [Fact]
        public async Task Test_Second_Service_Not_Healthy_While_First_Running()
        {
            var storageProvider = new LowLevelInMemoryStorageProvider<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorageProvider<Demodto>>>());
            var (factory1, _, _) = await CreateWebhostFactoryAsync(storageProvider: storageProvider); //and wait until healthy

            // Start the second host
            (var factory2, HttpClient healthClient2, _) = await CreateWebhostFactoryAsync(storageProvider: storageProvider, false);
            await Task.Delay(TimeSpan.FromSeconds(3));


            // Check that the second host is not healthy
            var healthResponse2 = await healthClient2.GetAsync("/healthz");
            var content = await healthResponse2.Content.ReadAsStringAsync();
            content.Should().Be("Unhealthy", "Second container should not be healthy while first is running");

            // Stop the first host
            await factory1.DisposeAsync();


            // Wait for the second host to become healthy
            var isHealthy = false;
            for (int i = 0; i < 50; i++)
            {
                healthResponse2 = await healthClient2.GetAsync("/healthz");
                content = await healthResponse2.Content.ReadAsStringAsync();
                if (content == "Healthy")
                {
                    isHealthy = true;
                    break;
                }
                await Task.Delay(TimeSpan.FromSeconds(0.5));
            }

            isHealthy.Should().BeTrue("Second host should become healthy after first is stopped");

            await factory2.DisposeAsync();
        }

        [Fact]
        public async Task Test_Service_Started_And_Healthy()
        {
            var storageProvider = new LowLevelInMemoryStorageProvider<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorageProvider<Demodto>>>());
            var createHostAction = async () => await CreateWebhostFactoryAsync(storageProvider: storageProvider);
            await createHostAction.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Read_Raw_Snapshot_Data_Tests()
        {
            var storageProvider = new LowLevelInMemoryStorageProvider<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorageProvider<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storageProvider: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);

            // Load base payload
            var basePayload = new Demodto();
            // Perform initial gRPC calls with unique names and some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], SevenStrings[i]);
                var response = await writerClient.PutAsync(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            var storageConfig = new StorageConfig { CustomRootFolder = _customPrefix + "/customRootFolder/" };

            ulong latestSnapshotVersion;
            while (true)
            {
                var listOfSnapshots = await storageProvider.ListObjects(ObjectStorageProvider<Demodto>.GetObjectNamePrefix(storageConfig) + "snapshots/");

                if (listOfSnapshots.Any())
                {
                    latestSnapshotVersion = (ulong)listOfSnapshots.OrderBy(d => SharedMethods.ExtractNumber(d.ObjectKey))
                        .GroupBy(g => SharedMethods.ExtractNumber(g.ObjectKey)).Max(gr => gr.Key);

                    if (latestSnapshotVersion > 1)
                        break;
                }

                await Task.Delay(10);
            }

            await factory1.DisposeAsync();

            MessageParser<Demodto> protoParser = new(() => (Demodto)Activator.CreateInstance(typeof(Demodto))!);
            using var resultStream = new MemoryStream();
            await storageProvider.DownloadWithCallBack(ObjectStorageProvider<Demodto>.GetObjectName(latestSnapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig), x => x.CopyTo(resultStream));

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
            var storageProvider = new LowLevelInMemoryStorageProvider<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorageProvider<Demodto>>>());

            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storageProvider: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Load base payload
            var basePayload = new Demodto();

            // Perform initial gRPC calls with unique names and some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], SevenStrings[i]);
                var response = await writerClient.PutAsync(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            // Perform update gRPC calls with modified some_string values
            for (var i = 0; i < SevenUniqueNames.Count; i++)
            {
                var newString = $"updated_{SevenStrings[i]}";
                var updatedPayload = UpdatePayload(basePayload, SevenUniqueNames[i], newString);
                var response = await writerClient.PutAsync(updatedPayload);
                // Verify response
                VerifyResponseIsExpected(response, 0, 1).Should().BeTrue();
            }

            // So now we have dataset versions 1..14
            // We expect a snaphot 1, 6, 11 - but can't guarantee that because of the async nature. But at least one should appear at some point, if all works.
            var storageConfig = new StorageConfig { CustomRootFolder = _customPrefix + "/customRootFolder/" };

            var fetchSnapshots = Task.Run(async () =>
            {
                while (true)
                {
                    var snapshots = await storageProvider.ListObjects(ObjectStorageProvider<Demodto>.GetObjectNamePrefix(storageConfig) + "snapshots/");

                    if (snapshots.Any())
                        return snapshots.Count;

                    // Prevent an absolute racing thread
                    await Task.Delay(10);
                }
            });
            var snapshotCount = await fetchSnapshots.WaitAsync(TimeSpan.FromSeconds(15));
            snapshotCount.Should().BeInRange(1, 3, "Ideally we get 1, 6, 11; but also valid is just 14 on the other end of the spectrum. More than 3 is never good as the interval should be 5 at least");

            // act
            await factory1.DisposeAsync();

            // assert
            var action = async (int i = 0) => await readerClient.GetDemodtoAsync(new GetDemodtoRequest { Name = SevenUniqueNames[i] });
            Func<Task> actionWrapper = async () => await action();
            await actionWrapper.Should().ThrowAsync<RpcException>();

            //more action
            (_, _, var newChannel) = await CreateWebhostFactoryAsync(storageProvider);

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