using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace RxDatasets.Reporting.Cqs.Tests;

internal sealed class RealCqsHost : IAsyncDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "rxdatasets-cqs-e2e-" + Guid.NewGuid().ToString("N"));
    private readonly ConcurrentQueue<string> _log = new();
    private readonly string? _image = Environment.GetEnvironmentVariable("CQS_TEST_IMAGE");
    private readonly string? _executable = Environment.GetEnvironmentVariable("CQS_TEST_EXECUTABLE");
    private readonly int _grpcPort = FreePort();
    private readonly int _healthPort = FreePort();
    private Process? _process;
    private IContainer? _container;
    private string _healthEndpoint = "";

    public string Endpoint { get; private set; } = "";
    public string Log => string.Join(Environment.NewLine, _log);

    public async Task Start(CancellationToken cancellation)
    {
        Directory.CreateDirectory(Path.Combine(_directory, "storage"));
        string config = Path.Combine(_directory, "cqs.toml");
        bool containerMode = !string.IsNullOrWhiteSpace(_image);
        string storage = containerMode ? "/cqs-test/storage" : Path.Combine(_directory, "storage").Replace('\\', '/');
        await File.WriteAllTextAsync(config, $"""
            [auth]
            skip_validation = true
            [server]
            grpc_addr = "0.0.0.0:{(containerMode ? 50051 : _grpcPort)}"
            health_addr = "0.0.0.0:{(containerMode ? 8081 : _healthPort)}"
            [persistence]
            store_url = "{storage}"
            interval = "100ms"
            [queues.demodto]
            dto_types = ["hightechinnovators.dtos.demodto.v1.Demodto"]
            [queues.demodto2]
            dto_types = ["hightechinnovators.dtos.demodto2.v1.Demodto2"]
            """, cancellation);

        if (containerMode)
        {
            _container ??= new ContainerBuilder().WithImage(_image!)
                .WithBindMount(_directory, "/cqs-test")
                .WithEnvironment("CQS_CONFIG", "/cqs-test/cqs.toml")
                .WithPortBinding(_grpcPort, 50051).WithPortBinding(_healthPort, 8081)
                .Build();
            await _container.StartAsync(cancellation);
            Endpoint = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(50051)}";
            _healthEndpoint = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(8081)}/health/ready";
        }
        else
        {
            Endpoint = $"http://127.0.0.1:{_grpcPort}";
            _healthEndpoint = $"http://127.0.0.1:{_healthPort}/health/ready";
        }

        using HttpClient health = new() { Timeout = TimeSpan.FromSeconds(1) };
        using CancellationTokenSource readiness = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        readiness.CancelAfter(TimeSpan.FromSeconds(75));
        while (true)
        {
            readiness.Token.ThrowIfCancellationRequested();
            if (!containerMode && (_process == null || _process.HasExited))
            {
                _process?.Dispose();
                ProcessStartInfo start = new(_executable!)
                {
                    WorkingDirectory = _directory, UseShellExecute = false,
                    RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true
                };
                foreach (string key in start.Environment.Keys.Where(key => key.StartsWith("CQS__", StringComparison.OrdinalIgnoreCase)).ToArray())
                {
                    start.Environment.Remove(key);
                }
                start.Environment["CQS_CONFIG"] = config;
                _process = new Process { StartInfo = start };
                _process.OutputDataReceived += (_, args) => { if (args.Data != null) _log.Enqueue(args.Data); };
                _process.ErrorDataReceived += (_, args) => { if (args.Data != null) _log.Enqueue(args.Data); };
                _process.Start();
                _process.BeginOutputReadLine();
                _process.BeginErrorReadLine();
            }

            try
            {
                using HttpResponseMessage response = await health.GetAsync(_healthEndpoint, readiness.Token);
                if (response.IsSuccessStatusCode) return;
            }
            catch (HttpRequestException) { }
            catch (OperationCanceledException) when (!readiness.IsCancellationRequested) { }
            await Task.Delay(250, readiness.Token);
        }
    }

    public async Task Stop(CancellationToken cancellation)
    {
        if (_container != null)
        {
            await _container.StopAsync(cancellation);
            (string stdout, string stderr) = await _container.GetLogsAsync(ct: cancellation);
            _log.Enqueue(stdout + stderr);
        }
        if (_process is { HasExited: false })
        {
            _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync(cancellation);
        }
    }

    public async ValueTask DisposeAsync()
    {
        using CancellationTokenSource cleanup = new(TimeSpan.FromSeconds(10));
        try
        {
            await Stop(cleanup.Token);
            if (_container != null)
            {
                await _container.DisposeAsync().AsTask().WaitAsync(cleanup.Token);
            }
        }
        finally
        {
            _process?.Dispose();
            try { Directory.Delete(_directory, recursive: true); }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    private static int FreePort()
    {
        using TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}