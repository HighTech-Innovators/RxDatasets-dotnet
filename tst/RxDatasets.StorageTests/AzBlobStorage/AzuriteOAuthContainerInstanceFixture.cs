using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using DotNet.Testcontainers.Images;
using Testcontainers.Azurite;

namespace RxDatasets.StorageTests.AzBlobStorage;

/// <summary>
/// Starts Azurite with OAuth ("--oauth basic") and a locally generated, self-signed TLS
/// certificate, since Azurite rejects Bearer tokens over plain HTTP. Azurite's "basic" OAuth
/// level does not cryptographically validate the token, so any well-formed JWT is accepted -
/// but the client still needs to trust the container's certificate, see
/// <see cref="AzStorageManagedIdentityTests"/> for the one-time setup this requires.
/// </summary>
public class AzuriteOAuthContainerInstanceFixture : IClassFixture<AzuriteContainer>
{
    public const int BlobPort = 10000;

    /// <summary>
    /// Fixed, reused across test runs so a developer only has to trust it once (see
    /// <see cref="AzStorageManagedIdentityTests"/>) instead of re-trusting a fresh certificate every run.
    /// </summary>
    private static readonly string CertificatePath = Path.Combine(Path.GetTempPath(), "rxdatasets-azurite-oauth-test-cert.pfx");

    public AzuriteContainer Container { get; }

    public X509Certificate2 Certificate { get; }

    public AzuriteOAuthContainerInstanceFixture()
    {
        Certificate = LoadOrCreateCertificate();

        Container = new AzuriteBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite:latest")
            .WithImagePullPolicy(PullPolicy.Always)
            .WithResourceMapping(Encoding.ASCII.GetBytes(Certificate.ExportCertificatePem()), "/certs/cert.pem")
            .WithResourceMapping(Encoding.ASCII.GetBytes(Certificate.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem()), "/certs/key.pem")
            // AzuriteBuilder already applies its own "-l"/host-binding args - only add the oauth/cert flags here,
            // since re-specifying the others causes a duplicate-flag conflict that silently drops the host bindings.
            .WithCommand("--oauth", "basic", "--cert", "/certs/cert.pem", "--key", "/certs/key.pem")
            .Build();
    }

    public Task DisposeAsync()
    {
        Certificate.Dispose();
        return Task.CompletedTask;
    }

    private static X509Certificate2 LoadOrCreateCertificate()
    {
        if (File.Exists(CertificatePath))
        {
            var existing = X509CertificateLoader.LoadPkcs12FromFile(CertificatePath, password: null);
            if (existing.NotAfter > DateTimeOffset.UtcNow.AddDays(1))
                return existing;
            existing.Dispose();
        }

        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=127.0.0.1", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddYears(2));

        File.WriteAllBytes(CertificatePath, certificate.Export(X509ContentType.Pfx));
        return certificate;
    }
}
