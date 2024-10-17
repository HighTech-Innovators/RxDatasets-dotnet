using System.Diagnostics;
using System.Text.RegularExpressions;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using RxDatasets.StorageProvider.Minio;

namespace RxDatasets.StorageTests;

public class DemodtoHostContainerFixture : IAsyncLifetime
{
    public IContainer? DemoDtoHostContainerInstance  { get; private set; }
    private string _hostImageName = "";

    public async Task InitializeAsync()
    {
        Console.WriteLine("creating build image!");
        var contextPath = CommonDirectoryPath.GetSolutionDirectory().DirectoryPath.ToLower();
        string? dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");
        string dockerhostarg = dockerHost is null ? "" : $"--build-arg DOCKER_HOST={dockerHost}";
        var arguments =
            $"build --build-arg VERSION=0.0.1 --progress=plain --target final-host {dockerhostarg} {contextPath} ";
        await BuildImageUsingCli("docker", arguments);
        Console.WriteLine("finished building image!");

    }

    public async Task BuildImageUsingCli(string executableName, string arguments)
    {
        var processStartInfo = new ProcessStartInfo
        {
            FileName = executableName,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = new Process();
        process.StartInfo = processStartInfo;
        process.Start();

        // Asynchronously read the standard output and standard error of the process
        var outputStringTask = process.StandardOutput.ReadToEndAsync();
        var errorStringTask = process.StandardError.ReadToEndAsync();

        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(300));

        Console.WriteLine("Waiting for the process to finish");
        await process.WaitForExitAsync(cts.Token);

        Console.WriteLine("Waiting for stdout to finish");
        var outputString = await outputStringTask;

        Console.WriteLine("Waiting for stderr to finish");
        var errorString = await errorStringTask;


        // You can check the output or error strings for information about the process execution
        if (process.ExitCode != 0)
        {
            throw new Exception($"Failed to build image: \n{errorString}");
        }

        _hostImageName = ExtractDockerTag(outputString + errorString);
    }

    private static string ExtractDockerTag(string input)
    {
        return TryGetLastSha256(input) ?? string.Empty;
    }

    private static string? TryGetLastSha256(string input)
    {
        const string pattern = @"sha256:[a-fA-F0-9]{64}";
        var matches = Regex.Matches(input, pattern);
        return matches.Count > 0 ? matches[^1].Value : null;
    }

    public async Task StartContainer(MinioS3Config? s3Config, INetwork network, string bucketName, bool waitUntilHealthy = true)
    {
        Console.WriteLine("start container!");

        if (_hostImageName==string.Empty) throw new Exception("Docker image sha was not found after building without errors");

        ContainerBuilder containerBuilder = new ContainerBuilder()
                    .WithImage(_hostImageName)
                    .WithNetwork(network)
                    .WithEnvironment("MINIOS3__ACCESSKEY", s3Config?.AccessKey)
                    .WithEnvironment("MINIOS3__SECRETKEY", s3Config?.SecretKey)
                    .WithEnvironment("MINIOS3__SERVICEURL", s3Config?.ServiceURL)
                    .WithEnvironment("MINIOS3__BucketName", bucketName)
                    .WithEnvironment("S3__ACCESSKEY", s3Config?.AccessKey)
                    .WithEnvironment("S3__SECRETKEY", s3Config?.SecretKey)
                    .WithEnvironment("S3__SERVICEURL", s3Config?.ServiceURL)
                    .WithEnvironment("S3__BucketName", bucketName)
                    .WithEnvironment("Storage__CustomRootFolder", "customRootFolder/")
                    .WithEnvironment("SnapshotCreation__SnapshotInterval", "5")
                    .WithPortBinding(10080, true)
                    .WithPortBinding(18080, true)
                    .WithEntrypoint("/bin/dtoserver/Host");

        if (waitUntilHealthy)
        {
            containerBuilder = containerBuilder.WithWaitStrategy(Wait.ForUnixContainer().UntilContainerIsHealthy(10));
        }

        DemoDtoHostContainerInstance = containerBuilder.Build();
        await DemoDtoHostContainerInstance.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (DemoDtoHostContainerInstance != null)
        {
            await DemoDtoHostContainerInstance.DisposeAsync();
        }
    }
}