using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.StorageProvider.AzBlob;

namespace RxDatasets.StorageTests.AzBlobStorage;

/// <summary>
/// Exercises AzStorage's AccountUrl + DefaultAzureCredential code path (the managed identity /
/// Azure AD auth mode, as opposed to a connection string) against a local Azurite container.
///
/// This is a manual, opt-in test - it is skipped by default because it needs a real Azure AD
/// identity to sign in with, which isn't available in CI. Azurite's "--oauth basic" mode does not
/// cryptographically validate the token, so DefaultAzureCredential's AzureCliCredential fallback
/// (i.e. a plain `az login`) is enough; no real Azure Storage resource or RBAC role is required.
///
/// One-time local setup:
///   1. `az login` (so DefaultAzureCredential can obtain a token via AzureCliCredential).
///   2. Run this test once to generate the self-signed certificate at
///      %TEMP%/rxdatasets-azurite-oauth-test-cert.pfx (see AzuriteOAuthContainerInstanceFixture).
///   3. Trust that certificate in your OS/.NET trust store, e.g. on Linux:
///        openssl pkcs12 -in /tmp/rxdatasets-azurite-oauth-test-cert.pfx -nokeys -out azurite.crt -passin pass:
///        sudo cp azurite.crt /usr/local/share/ca-certificates/azurite-test.crt
///        sudo update-ca-certificates
///      (On macOS: add to Keychain and trust for SSL. On Windows: import into "Trusted Root
///      Certification Authorities" via certmgr.)
///   4. Remove the [Fact(Skip = ...)] attribute below and run the test.
/// </summary>
public class AzStorageManagedIdentityTests : IClassFixture<AzuriteOAuthContainerInstanceFixture>, IAsyncLifetime
{
    private readonly AzuriteOAuthContainerInstanceFixture _fixture;

    public AzStorageManagedIdentityTests(AzuriteOAuthContainerInstanceFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        if (_fixture.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await _fixture.Container.StartAsync();
    }

    public Task DisposeAsync() => _fixture.DisposeAsync();

    [Fact(Skip = "Manual integration test - requires az login, see class summary for one-time setup")]
    public async Task Should_Authenticate_And_Write_Via_AccountUrl_And_DefaultAzureCredential()
    {
        var accountUrl = $"https://127.0.0.1:{_fixture.Container.GetMappedPublicPort(AzuriteOAuthContainerInstanceFixture.BlobPort)}/devstoreaccount1";

        var config = Options.Create(new AzStorageConfig
        {
            AccountUrl = accountUrl,
            ContainerName = nameof(Should_Authenticate_And_Write_Via_AccountUrl_And_DefaultAzureCredential).ToLower()
        });

        var initializeAndUploadAction = async () =>
        {
            var storage = new AzStorage<object>(Mock.Of<ILogger<AzStorage<object>>>(), config, retryInitialization: false);
            await storage.UploadBytes("managed-identity-smoke-test", [1, 2, 3]);
        };

        await initializeAndUploadAction.Should().NotThrowAsync();
    }
}
