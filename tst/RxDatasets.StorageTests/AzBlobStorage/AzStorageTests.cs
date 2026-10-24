using System.Reflection;
using Azure.Identity;
using FluentAssertions;
using RxDatasets.StorageProvider.AzBlob;

namespace RxDatasets.StorageTests.AzBlobStorage;

public class AzStorageTests
{
    // AzStorage<T>'s constructor unconditionally kicks off Initialize()/InitializationLoop(), which drives
    // DefaultAzureCredential's real credential chain (incl. a network probe to the Azure IMDS endpoint) before
    // it ever touches AccountUrl. There is no way to construct AzStorage without triggering that, so these tests
    // call the private CreateCredential method directly instead - building a DefaultAzureCredential is pure,
    // in-memory object construction with no I/O; only calling GetToken on it would touch the network.
    private static DefaultAzureCredential InvokeCreateCredential(AzStorageConfig config)
    {
        var method = typeof(AzStorage<object>).GetMethod("CreateCredential", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (DefaultAzureCredential)method.Invoke(null, [config])!;
    }

    [Fact]
    public void Should_Create_Credential_Without_Throwing_When_ManagedIdentityClientId_Is_Not_Set()
    {
        var config = new AzStorageConfig { AccountUrl = "https://account.blob.core.windows.net", ContainerName = "container" };

        var createCredential = () => InvokeCreateCredential(config);

        createCredential.Should().NotThrow();
    }

    [Fact]
    public void Should_Create_Credential_Without_Throwing_When_ManagedIdentityClientId_Is_Set()
    {
        var config = new AzStorageConfig
        {
            AccountUrl = "https://account.blob.core.windows.net",
            ContainerName = "container",
            ManagedIdentityClientId = "00000000-0000-0000-0000-000000000000"
        };

        var createCredential = () => InvokeCreateCredential(config);

        createCredential.Should().NotThrow();
    }
}
