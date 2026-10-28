using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.Tests;

/// <summary>
/// A validated configuration section says which keys it bound, never their values (VLOW-37).
/// </summary>
[Collection(nameof(ConsoleOutputCollection))]
public class ConfigExtensionsTests
{
    private const string Secret = "do-not-print-me-3f9c";

    public class SecretConfig
    {
        public string AccessKey { get; set; } = "";
        public string SecretKey { get; set; } = "";
    }

    private static string StartWith(string sectionKey)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{sectionKey}:AccessKey"] = "some-access-key",
                [$"{sectionKey}:SecretKey"] = Secret,
            })
            .Build();
        var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddValidatedConfigSection<SecretConfig>(sectionKey);

        var original = Console.Out;
        using var captured = new StringWriter();
        Console.SetOut(captured);
        try
        {
            using var provider = services.BuildServiceProvider();
            provider.GetRequiredService<IOptions<SecretConfig>>().Value.SecretKey.Should().Be(Secret);
        }
        finally
        {
            Console.SetOut(original);
        }

        return captured.ToString();
    }

    [Fact]
    public void BindingASectionShouldNotWriteItsValues()
    {
        // The set of keys already written is static, so each test uses a section key of its own.
        var output = StartWith("VLOW37NoValues");

        output.Should().NotContain(Secret);
        output.Should().NotContain("some-access-key");
    }

    [Fact]
    public void BindingASectionShouldStillNameItsKeys()
    {
        var output = StartWith("VLOW37Keys");

        output.Should().Contain("VLOW37Keys__SecretKey").And.Contain("VLOW37Keys__AccessKey");
    }

    private static void Bind(string sectionKey)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { [$"{sectionKey}:AccessKey"] = "key" })
            .Build();
        using var provider = new ServiceCollection()
            .AddSingleton<IConfiguration>(configuration)
            .AddValidatedConfigSection<SecretConfig>(sectionKey)
            .BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<SecretConfig>>().Value;
    }

    private static string WithConsoleCaptured(Action act)
    {
        var original = Console.Out;
        using var captured = new StringWriter();
        Console.SetOut(captured);
        try
        {
            act();
        }
        finally
        {
            Console.SetOut(original);
        }

        return captured.ToString();
    }

    [Fact]
    public void BindingSectionsOnManyThreadsAtOnceShouldNotCorruptTheKeysAlreadyWritten()
    {
        // Hosts started in parallel, as the storage tests do, bind their sections at the same time.
        var prefix = $"Race{Guid.NewGuid():N}";

        var binding = () => WithConsoleCaptured(() =>
            Parallel.For(0, 2000, new ParallelOptions { MaxDegreeOfParallelism = 16 }, i => Bind($"{prefix}-{i}")));

        binding.Should().NotThrow();
    }

    [Fact]
    public void BindingOneSectionOnManyThreadsAtOnceShouldNameItsKeysOnce()
    {
        var sectionKey = $"Once{Guid.NewGuid():N}";

        var output = WithConsoleCaptured(() =>
            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ => Bind(sectionKey)));

        output.Split($"{sectionKey}__AccessKey").Length.Should().Be(2, "the keys of a section are written once");
    }
}

/// <summary>Tests that redirect <see cref="Console.Out"/> must not run alongside each other.</summary>
[CollectionDefinition(nameof(ConsoleOutputCollection), DisableParallelization = true)]
public class ConsoleOutputCollection;
