using System.ComponentModel.DataAnnotations;
using FluentAssertions;
using RxDatasets.StorageProvider.AzBlob;

namespace RxDatasets.StorageTests.AzBlobStorage;

public class AzStorageConfigTests
{
    private static bool TryValidate(AzStorageConfig config, out List<ValidationResult> results)
    {
        results = [];
        var context = new ValidationContext(config);
        return Validator.TryValidateObject(config, context, results, true);
    }

    [Fact]
    public void Should_Fail_Validation_When_Neither_ConnectionString_Nor_AccountUrl_Is_Set()
    {
        var config = new AzStorageConfig { ContainerName = "container" };

        var isValid = TryValidate(config, out var results);

        isValid.Should().BeFalse();
        results.Should().ContainSingle(r => r.ErrorMessage!.Contains("Either ConnectionString or AccountUrl must be filled."));
    }

    [Fact]
    public void Should_Fail_Validation_When_Both_ConnectionString_And_AccountUrl_Are_Set()
    {
        var config = new AzStorageConfig
        {
            ContainerName = "container",
            ConnectionString = "UseDevelopmentStorage=true",
            AccountUrl = "https://account.blob.core.windows.net"
        };

        var isValid = TryValidate(config, out var results);

        isValid.Should().BeFalse();
        results.Should().ContainSingle(r => r.ErrorMessage!.Contains("but not both"));
    }

    [Fact]
    public void Should_Pass_Validation_When_Only_ConnectionString_Is_Set()
    {
        var config = new AzStorageConfig
        {
            ContainerName = "container",
            ConnectionString = "UseDevelopmentStorage=true"
        };

        var isValid = TryValidate(config, out var results);

        isValid.Should().BeTrue();
        results.Should().BeEmpty();
    }

    [Fact]
    public void Should_Pass_Validation_When_Only_AccountUrl_Is_Set()
    {
        var config = new AzStorageConfig
        {
            ContainerName = "container",
            AccountUrl = "https://account.blob.core.windows.net",
            ManagedIdentityClientId = "00000000-0000-0000-0000-000000000000"
        };

        var isValid = TryValidate(config, out var results);

        isValid.Should().BeTrue();
        results.Should().BeEmpty();
    }
}
