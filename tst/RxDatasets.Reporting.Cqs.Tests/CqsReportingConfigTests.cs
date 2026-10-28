using System.ComponentModel.DataAnnotations;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RxDatasets.BackgroundServices;
using RxDatasets.StorageProviders;
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
        config.ReportingIdentifier.Should().Be("cqs-reporter");
        IsValid(config).Should().BeFalse();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void RejectsAnEmptyReportingIdentifier(string identifier) =>
        IsValid(new CqsReportingConfig { Endpoint = "http://localhost:5000", ReportingIdentifier = identifier })
            .Should().BeFalse();

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

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, false)]
    public void DurabilityTimeoutMustFitTheRequestsMillisecondField(int extraMilliseconds, bool valid) =>
        IsValid(new CqsReportingConfig
        {
            Endpoint = "http://localhost:5000",
            DurabilityTimeout = TimeSpan.FromMilliseconds(int.MaxValue) + TimeSpan.FromMilliseconds(extraMilliseconds)
        }).Should().Be(valid);

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
        DtoServerBuilder<Demodto> builder = new(services);
        builder.WithInMemoryDataset().WithCqsReporting().Should().BeSameAs(builder);
        builder.Build();
        await using ServiceProvider provider = services.BuildServiceProvider();
        CqsReporter<Demodto> reporter = provider.GetRequiredService<CqsReporter<Demodto>>();
        provider.GetRequiredService<CqsReporter<Demodto>>().Should().BeSameAs(reporter);
        reporter.MaxDeltasPerTransaction.Should().Be(17);
        CqsReportingConfig config = provider.GetRequiredService<IOptions<CqsReportingConfig>>().Value;
        config.DurabilityTimeout.Should().Be(TimeSpan.FromSeconds(4));
        services.Should().Contain(descriptor => descriptor.ServiceType ==
            typeof(DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>>));
        services.Should().ContainSingle(descriptor => IsCqsConsumer(descriptor));
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
        new DtoServerBuilder<Demodto>(services).WithCqsReporting();
        using ServiceProvider provider = services.BuildServiceProvider();
        FluentActions.Invoking(() => provider.GetRequiredService<CqsReporter<Demodto>>())
            .Should().Throw<OptionsValidationException>();
    }

    [Fact]
    public void ConsumerIsRegisteredAfterTheLockAndStorageSoItStopsBeforeThem()
    {
        // Hosted services stop in reverse registration order: the consumer's last cursor write needs the lock and storage.
        ServiceCollection services = new();
        DtoServerBuilder<Demodto> builder = new DtoServerBuilder<Demodto>(services).WithDistributedLock().WithCqsReporting();
        services.Should().NotContain(descriptor => IsCqsConsumer(descriptor), "the consumer is registered by Build()");
        builder.Build();

        int consumer = services.IndexOf(services.Single(IsCqsConsumer));
        consumer.Should().BeGreaterThan(IndexOfSingleton(services, typeof(DistributedLockService<Demodto>)))
            .And.BeGreaterThan(IndexOfSingleton(services, typeof(ObjectStorageProvider<Demodto>)));
        FluentActions.Invoking(() => builder.RegisterLast(_ => { })).Should().Throw<InvalidOperationException>();
    }

    private static bool IsCqsConsumer(ServiceDescriptor descriptor) =>
        descriptor.ServiceType == typeof(IHostedService) && descriptor.ImplementationType ==
        typeof(DatasetUpdateConsumingService<Demodto, DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>>>);

    private static int IndexOfSingleton(ServiceCollection services, System.Type type) =>
        services.IndexOf(services.Single(descriptor => descriptor.ServiceType == type));

    private static bool IsValid(CqsReportingConfig config) =>
        Validator.TryValidateObject(config, new ValidationContext(config), new List<ValidationResult>(), true);
}