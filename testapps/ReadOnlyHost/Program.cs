using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ReadOnlyHost;

public static class Program
{
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);

        if (!builder.Environment.IsEnvironment("Testing"))
        {
            builder.WebHost.ConfigureKestrel(options =>
            {
                // Setup a HTTP/2 endpoint without TLS.
                options.Listen(IPAddress.Any, 18081, o => o.Protocols = HttpProtocols.Http1); // grpc-web, health checks, websockets
                options.Listen(IPAddress.Any, 10081, o => o.Protocols = HttpProtocols.Http2); // grpc over HTTP/2, no TLS
            });
        }

        DI.ConfigureServices(builder.Services);

        var app = builder.Build();

        DI.ConfigureApp(app);

        var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();

        lifetime.ApplicationStarted.Register(() =>
        {
            // Run a health check here to allow to app to instantiate before other calls
            Task.Run(async () =>
            {
                using var client = new HttpClient();
                await client.GetAsync("http://localhost:18081/healthz");
                await Task.Delay(TimeSpan.FromSeconds(5));
                await client.GetAsync("http://localhost:18081/healthz");
            });
        });

        app.Run();
    }
}
