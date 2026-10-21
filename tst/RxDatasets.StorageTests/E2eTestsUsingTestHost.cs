using System.Runtime.CompilerServices;
using FluentAssertions;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Web;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;
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

        private async Task<(WebApplicationFactory<Program> factory, HttpClient healthClient, GrpcChannel channel)> CreateWebhostFactoryAsync(IDiskStorage<Demodto> storage, bool waitForHealthy = true, GrpcWebApplicationFactory? factoryInput = null, [CallerMemberName] string memberName = "")
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
                    services.AddSingleton(storage);
                });
            });

            var healthClient = customFactory.CreateClient();
            // Binary gRPC-Web, as real clients use: with GrpcWebText, a streaming call over the TestServer
            // intermittently failed decoding its response ("Unexpected end of data when reading base64 content")
            var grpcWebHandler = new GrpcWebHandler(GrpcWebMode.GrpcWeb, customFactory.Server.CreateHandler());
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
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, _) = await CreateWebhostFactoryAsync(storage: storageProvider); //and wait until healthy

            // Start the second host
            (var factory2, HttpClient healthClient2, _) = await CreateWebhostFactoryAsync(storage: storageProvider, false);
            //await Task.Delay(TimeSpan.FromSeconds(3));


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
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var createHostAction = async () => await CreateWebhostFactoryAsync(storage: storageProvider);
            await createHostAction.Should().NotThrowAsync();
        }

        [Fact]
        public async Task Read_Raw_Snapshot_Data_Tests()
        {
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

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
                var listOfSnapshots = await storageProvider.ListObjects(DatasetHelper.GetObjectNamePrefix<Demodto>(storageConfig) + FolderNames.SnapshotsFolderNameWithSlash);

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
            await storageProvider.DownloadWithCallBack(DatasetHelper.GetObjectName<Demodto>(latestSnapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, storageConfig), x => x.CopyTo(resultStream));

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
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());

            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

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
                    List<(string ObjectKey, DateTimeOffset DateTime)> snapshots =
                        await storageProvider.ListObjects(
                            DatasetHelper.GetObjectNamePrefix<Demodto>(storageConfig) + FolderNames.SnapshotsFolderNameWithSlash);

                    if (snapshots.Any())
                    {
                        var snapshotVersions = snapshots
                            .GroupBy(g => SharedMethods.ExtractNumber(g.ObjectKey)).Select(g => g.First());
                        return snapshotVersions.Count();
                    }



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

        [Fact]
        public async Task Test_PutOrDeleteMany_DeletePutSameEntity_ReportsNoChange()
        {
            // Integration test: delete followed by put of same entity should report no change
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Arrange: Create initial entity using proper gRPC
            var basePayload = new Demodto { Name = "test_entity_delete_put_same", SomeString = "original_data" };
            var putResponse = await writerClient.PutAsync(basePayload);
            VerifyResponseIsExpected(putResponse, 1, 0).Should().BeTrue("Initial put should add one entity");

            // Add small delay to ensure initial put is fully processed
            await Task.Delay(100);

            try
            {
                // Act: Delete then put same entity in batch using PutOrDeleteMany gRPC streaming
                var call = writerClient.PutOrDeleteMany();

                // Delete operation
                await call.RequestStream.WriteAsync(new PutOrDelete
                {
                    Name = basePayload.Name,
                    IsDelete = true
                });

                // Small delay between operations
                await Task.Delay(50);

                // Put back same entity
                await call.RequestStream.WriteAsync(new PutOrDelete
                {
                    Name = basePayload.Name,
                    EntityBytes = basePayload.ToByteString()
                });

                await call.RequestStream.CompleteAsync();

                // Add delay before reading response
                await Task.Delay(100);

                var batchResponse = await call;

                // Assert: Should report no net change
                batchResponse.Should().NotBeNull();
                batchResponse.NumAddedEntities.Should().Be(0u, "No entities should be added when delete+put results in same state");
                batchResponse.NumUpdatedEntities.Should().Be(0u, "No entities should be updated when delete+put results in same state");
                batchResponse.NumDeletedEntities.Should().Be(0u, "No entities should be deleted when delete+put results in same state");

                // Verify entity still exists with original data using gRPC reader
                var retrievedEntity = await readerClient.GetDemodtoAsync(new GetDemodtoRequest { Name = basePayload.Name });
                retrievedEntity.Should().NotBeNull("Entity should still exist after delete+put with same data");
                retrievedEntity.SomeString.Should().Be(basePayload.SomeString, "Entity data should be unchanged");
            }
            catch (Exception ex)
            {
                // Add detailed error information
                throw new Exception($"gRPC streaming test failed. Error: {ex.Message}. InnerException: {ex.InnerException?.Message}", ex);
            }
            finally
            {
                await factory1.DisposeAsync();
            }
        }

        [Fact]
        public async Task Test_PutOrDeleteMany_DeletePutDifferentEntity_ReportsChange()
        {
            // Integration test: delete followed by put of different entity should report a change
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Arrange: Create initial entity using proper gRPC
            var basePayload = new Demodto { Name = "test_entity_delete_put_different", SomeString = "original_data" };
            var putResponse = await writerClient.PutAsync(basePayload);
            VerifyResponseIsExpected(putResponse, 1, 0).Should().BeTrue("Initial put should add one entity");

            // Act: Delete then put different entity in batch using PutOrDeleteMany gRPC streaming
            var modifiedPayload = new Demodto { Name = basePayload.Name, SomeString = "modified_data" };
            var call = writerClient.PutOrDeleteMany();

            // Delete operation
            await call.RequestStream.WriteAsync(new PutOrDelete
            {
                Name = basePayload.Name,
                IsDelete = true
            });

            // Put back different entity
            await call.RequestStream.WriteAsync(new PutOrDelete
            {
                Name = modifiedPayload.Name,
                EntityBytes = modifiedPayload.ToByteString()
            });

            await call.RequestStream.CompleteAsync();
            var batchResponse = await call;

            // Assert: Should report a change
            batchResponse.Should().NotBeNull();
            batchResponse.NumAddedEntities.Should().Be(0u, "Should not count as added since entity existed before");
            batchResponse.NumUpdatedEntities.Should().Be(1u, "Should count as updated since final data differs from initial");
            batchResponse.NumDeletedEntities.Should().Be(0u, "Should not count as deleted since entity exists after batch");

            // Verify entity exists with modified data using gRPC reader
            var retrievedEntity = await readerClient.GetDemodtoAsync(new GetDemodtoRequest { Name = basePayload.Name });
            retrievedEntity.Should().NotBeNull("Entity should exist after delete+put with different data");
            retrievedEntity.SomeString.Should().Be(modifiedPayload.SomeString, "Entity should have the modified data");

            await factory1.DisposeAsync();
        }

        [Fact]
        public async Task Test_PutOrDeleteMany_MultiplePutsOriginal_ReportsNoChange()
        {
            // Integration test: multiple operations ending with original state should report no change
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Arrange: Create initial entity using proper gRPC
            var basePayload = new Demodto { Name = "test_entity_multiple_puts_original", SomeString = "original_data" };
            var putResponse = await writerClient.PutAsync(basePayload);
            VerifyResponseIsExpected(putResponse, 1, 0).Should().BeTrue("Initial put should add one entity");

            // Add delay to ensure initial put is fully processed
            await Task.Delay(100);

            try
            {
                // Act: Multiple operations ending with original data
                var tempPayload1 = new Demodto { Name = basePayload.Name, SomeString = "temp_data_1" };
                var tempPayload2 = new Demodto { Name = basePayload.Name, SomeString = "temp_data_2" };
                var call = writerClient.PutOrDeleteMany();

                // Put different data
                await call.RequestStream.WriteAsync(new PutOrDelete
                {
                    Name = tempPayload1.Name,
                    EntityBytes = tempPayload1.ToByteString()
                });

                // Small delay between operations
                await Task.Delay(50);

                // Put more different data
                await call.RequestStream.WriteAsync(new PutOrDelete
                {
                    Name = tempPayload2.Name,
                    EntityBytes = tempPayload2.ToByteString()
                });

                // Small delay between operations
                await Task.Delay(50);

                // Put back original data
                await call.RequestStream.WriteAsync(new PutOrDelete
                {
                    Name = basePayload.Name,
                    EntityBytes = basePayload.ToByteString()
                });

                await call.RequestStream.CompleteAsync();

                // Add delay before reading response
                await Task.Delay(100);

                var batchResponse = await call;

                // Assert: Should report no net change
                // CRITICAL: This test MUST FAIL before the bug fix is implemented
                batchResponse.Should().NotBeNull();
                batchResponse.NumAddedEntities.Should().Be(0u, "No entities should be added when ending with original state");
                batchResponse.NumUpdatedEntities.Should().Be(0u, "No entities should be updated when ending with original state");
                batchResponse.NumDeletedEntities.Should().Be(0u, "No entities should be deleted when ending with original state");

                // Verify entity still exists with original data using gRPC reader
                var retrievedEntity = await readerClient.GetDemodtoAsync(new GetDemodtoRequest { Name = basePayload.Name });
                retrievedEntity.Should().NotBeNull("Entity should still exist with original data");
                retrievedEntity.SomeString.Should().Be(basePayload.SomeString, "Entity data should be back to original");
            }
            finally
            {
                await factory1.DisposeAsync();
            }
        }

        /// <summary>
        /// T020: Integration test for gRPC CountChildren with testHost
        /// Verifies that CountChildren RPC method works through gRPC ReaderService
        /// and returns correct counts for parent hierarchies
        /// </summary>
        [Fact]
        public async Task CountChildren_ReturnsCorrectCountForGivenParent()
        {
            // Arrange
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Create a parent-child hierarchy
            const string parentPath = "departments/engineering";
            var childEntities = new[]
            {
                new Demodto { Name = "departments/engineering/employees/alice", SomeString = "engineer" },
                new Demodto { Name = "departments/engineering/employees/bob", SomeString = "engineer" },
                new Demodto { Name = "departments/engineering/employees/charlie", SomeString = "engineer" },
            };

            // Write child entities
            foreach (var entity in childEntities)
            {
                var response = await writerClient.PutAsync(entity);
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            // Allow time for entities to be indexed
            await Task.Delay(100);

            // Act: Call CountChildren RPC
            var countRequest = new CountChildrenRequest { Parent = parentPath };
            var countResponse = await readerClient.CountChildrenAsync(countRequest);

            // Assert
            countResponse.Should().NotBeNull();
            countResponse.Count.Should().Be(3u, "Should count 3 children under departments/engineering");

            await factory1.DisposeAsync();
        }

        /// <summary>
        /// T020 (continued): Test CountChildren with no children returns 0
        /// </summary>
        [Fact]
        public async Task CountChildren_WithNoChildren_ReturnsZero()
        {
            // Arrange
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Act: Call CountChildren for a parent with no children
            const string emptyParentPath = "departments/nonexistent";
            var countRequest = new CountChildrenRequest { Parent = emptyParentPath };
            var countResponse = await readerClient.CountChildrenAsync(countRequest);

            // Assert
            countResponse.Should().NotBeNull();
            countResponse.Count.Should().Be(0u, "Should return 0 for parent with no children");

            await factory1.DisposeAsync();
        }

        /// <summary>
        /// T021: Integration test for concurrent gRPC CountChildren calls through testHost
        /// Verifies that multiple concurrent CountChildren calls return consistent results
        /// </summary>
        [Fact]
        public async Task CountChildren_ConcurrentCalls_ReturnConsistentResults()
        {
            // Arrange
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            const string parentPath = "teams/frontend";
            const int childCount = 10;

            // Write multiple child entities
            for (int i = 0; i < childCount; i++)
            {
                var entity = new Demodto
                {
                    Name = $"teams/frontend/developers/dev{i:D2}",
                    SomeString = $"developer_{i}"
                };
                var response = await writerClient.PutAsync(entity);
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            // Allow time for entities to be indexed
            await Task.Delay(100);

            // Act: Make multiple concurrent CountChildren calls
            var countTasks = Enumerable.Range(0, 5)
                .Select(_ => readerClient.CountChildrenAsync(new CountChildrenRequest { Parent = parentPath }).ResponseAsync)
                .ToList();

            var results = await Task.WhenAll(countTasks);

            // Assert: All concurrent calls should return the same count
            results.Should().AllSatisfy(count => count.Count.Should().Be((uint)childCount));

            await factory1.DisposeAsync();
        }

        /// <summary>
        /// T022: Integration test for gRPC CountChildren with different storage providers
        /// Note: Using InMemory for now; can be extended to test other providers
        /// </summary>
        [Fact]
        public async Task CountChildren_WithMultipleHierarchyLevels_CountsAllDescendants()
        {
            // Arrange
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Create entities at multiple hierarchy levels
            var entities = new[]
            {
                new Demodto { Name = "company/division/department", SomeString = "direct_child_1" },
                new Demodto { Name = "company/division/team", SomeString = "direct_child_2" },
                new Demodto { Name = "company/division/department/section", SomeString = "nested_child" },
                new Demodto { Name = "company/division/department/section/unit", SomeString = "deep_nested" },
            };

            foreach (var entity in entities)
            {
                var response = await writerClient.PutAsync(entity);
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            // Allow time for entities to be indexed
            await Task.Delay(100);

            // Act: Count children of "company/division"
            const string parentPath = "company/division";
            var countRequest = new CountChildrenRequest { Parent = parentPath };
            var countResponse = await readerClient.CountChildrenAsync(countRequest);

            // Assert: Should count all descendants under the parent path
            // This includes both direct children and nested descendants
            countResponse.Should().NotBeNull();
            countResponse.Count.Should().Be(4u, "Should count all descendants including nested ones");

            await factory1.DisposeAsync();
        }

        /// <summary>
        /// T026: End-to-end validation that CountChildren works through gRPC ReaderService
        /// Comprehensive test ensuring the feature integrates properly end-to-end
        /// </summary>
        [Fact]
        public async Task CountChildren_EndToEnd_VerifiesFeatureIntegration()
        {
            // Arrange
            var storageProvider = new LowLevelInMemoryStorage<Demodto>(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());
            var (factory1, _, channel) = await CreateWebhostFactoryAsync(storage: storageProvider);

            var writerClient = new WriterService.WriterServiceClient(channel);
            var readerClient = new ReaderService.ReaderServiceClient(channel);

            // Create initial data
            const string mainParent = "projects/mobile";
            var initialEntities = new[]
            {
                new Demodto { Name = "projects/mobile/ios/app", SomeString = "ios_app" },
                new Demodto { Name = "projects/mobile/android/app", SomeString = "android_app" },
            };

            foreach (var entity in initialEntities)
            {
                var response = await writerClient.PutAsync(entity);
                VerifyResponseIsExpected(response, 1, 0).Should().BeTrue();
            }

            await Task.Delay(100);

            // Act & Assert: Verify initial count
            var initialCount = await readerClient.CountChildrenAsync(new CountChildrenRequest { Parent = mainParent });
            initialCount.Count.Should().Be(2u);

            // Add more children
            var newEntity = new Demodto { Name = "projects/mobile/web/app", SomeString = "web_app" };
            await writerClient.PutAsync(newEntity);
            await Task.Delay(100);

            // Assert: Verify count increased
            var updatedCount = await readerClient.CountChildrenAsync(new CountChildrenRequest { Parent = mainParent });
            updatedCount.Count.Should().Be(3u, "Count should increase after adding new child");

            // Assert: Verify root path also works
            var rootCount = await readerClient.CountChildrenAsync(new CountChildrenRequest { Parent = "" });
            rootCount.Count.Should().BeGreaterThanOrEqualTo(3u, "Root path should count all top-level entities");

            await factory1.DisposeAsync();
        }
    }
}
