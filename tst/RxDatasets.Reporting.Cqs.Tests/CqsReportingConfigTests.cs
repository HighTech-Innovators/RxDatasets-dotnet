using System.ComponentModel.DataAnnotations;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RxDatasets.BackgroundServices;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

public class CqsReportingConfigTests
{
    [Fact]
    public void DefaultsMatchDurabilityContract()
    {
        CqsReportingConfig config = new();
        config.MaxDeltasPerTransaction.Should().Be(1000);
        config.MaxNotificationsPerPush.Should().Be(10000);
        config.DurabilityTimeout.Should().Be(TimeSpan.FromSeconds(35));
        IsValid(config).Should().BeFalse();
    }

    [Theory]
    [InlineData("")]
    [InlineData("relative/endpoint")]
    [InlineData("ftp://localhost:5000")]
    [InlineData("http://")]
    public void RejectsInvalidEndpoint(string endpoint) =>
        IsValid(new CqsReportingConfig { Endpoint = endpoint }).Should().BeFalse();

    [Theory]
    [InlineData("http://localhost:5000")]
    [InlineData("https://cqs.example.com")]
    public void AcceptsHttpAndHttps(string endpoint) =>
        IsValid(new CqsReportingConfig { Endpoint = endpoint }).Should().BeTrue();

    [Theory]
    [InlineData(0, 1, 35)]
    [InlineData(-1, 1, 35)]
    [InlineData(1, 0, 35)]
    [InlineData(1, -1, 35)]
    [InlineData(1, 1, 0)]
    [InlineData(1, 1, -1)]
    public void RejectsNonPositiveLimits(int deltas, int notifications, int timeoutSeconds)
    {
        IsValid(new CqsReportingConfig
        {
            Endpoint = "http://localhost:5000",
            MaxDeltasPerTransaction = deltas,
            MaxNotificationsPerPush = notifications,
            DurabilityTimeout = TimeSpan.FromSeconds(timeoutSeconds)
        }).Should().BeFalse();
    }

    [Fact]
    public void RejectsUnrepresentableDeadline()
    {
        IsValid(new CqsReportingConfig
        {
            Endpoint = "http://localhost:5000",
            DurabilityTimeout = TimeSpan.MaxValue
        }).Should().BeFalse();
    }

    [Fact]
    public async Task BuilderBindsConfigRegistersPipelineAndUsesGrpcClientFactory()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        ServiceCollection services = new();
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["CqsReporting:Endpoint"] = server.Endpoint,
            ["CqsReporting:MaxDeltasPerTransaction"] = "17",
            ["CqsReporting:MaxNotificationsPerPush"] = "1",
            ["CqsReporting:DurabilityTimeout"] = "00:00:04"
        }).Build();
        services.AddSingleton(configuration);
        DtoServerBuilder<Timestamp> builder = new(services);
        builder.WithCqsReporting().Should().BeSameAs(builder);
        await using ServiceProvider provider = services.BuildServiceProvider();
        CqsReporter<Timestamp> reporter = provider.GetRequiredService<CqsReporter<Timestamp>>();
        provider.GetRequiredService<CqsReporter<Timestamp>>().Should().BeSameAs(reporter);
        reporter.MaxDeltasPerTransaction.Should().Be(17);
        CqsReportingConfig config = provider.GetRequiredService<IOptions<CqsReportingConfig>>().Value;
        config.DurabilityTimeout.Should().Be(TimeSpan.FromSeconds(4));
        services.Should().Contain(descriptor => descriptor.ServiceType ==
            typeof(BatchingDatasetUpdateReportingPlugin<Timestamp, CqsReporter<Timestamp>>));
        services.Should().Contain(descriptor => descriptor.ServiceType == typeof(IHostedService)
            && descriptor.ImplementationType ==
            typeof(DatasetUpdateConsumingService<Timestamp,
                BatchingDatasetUpdateReportingPlugin<Timestamp, CqsReporter<Timestamp>>>));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("factory-client", null, null, transaction);
        server.Service.Requests.Should().ContainSingle();
        await reporter.CommitDeltaTransaction(transaction);
    }

    [Fact]
    public void BuilderRejectsInvalidBoundConfig()
    {
        ServiceCollection services = new();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["CqsReporting:Endpoint"] = "not-an-endpoint",
            ["CqsReporting:MaxNotificationsPerPush"] = "0"
        }).Build());
        new DtoServerBuilder<Timestamp>(services).WithCqsReporting();
        using ServiceProvider provider = services.BuildServiceProvider();
        FluentActions.Invoking(() => provider.GetRequiredService<CqsReporter<Timestamp>>())
            .Should().Throw<OptionsValidationException>();
    }

    private static bool IsValid(CqsReportingConfig config) =>
        Validator.TryValidateObject(config, new ValidationContext(config), new List<ValidationResult>(), true);
}