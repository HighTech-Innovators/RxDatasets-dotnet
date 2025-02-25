using System.Diagnostics;
using Grpc.Net.Client;
using Grpc.Net.Client.Web;
using Hightechinnovators.Dtos.DemodtoV1;
using TestClient;

//Hightechinnovators.Dtos.DemodtoV1.ReaderClient.DI.ConfigureServices();

if (!args.Any())
{
    Console.Error.WriteLine("Usage: TestClient host:port [-web|-web2]");
    Console.Error.WriteLine(" -web indicates to use grpc-web over http1");
    Console.Error.WriteLine(" -web2 indicates to use grpc-web over http2");
    return;
}
var address = args.FirstOrDefault(arg => arg.Contains(":")) ?? "localhost:10080";
bool grpcWebOverHttp2 = args.Any(arg => arg.Equals("-web2", StringComparison.InvariantCultureIgnoreCase));
bool grpcWeb = !grpcWebOverHttp2 && args.Any(arg => arg.Equals("-web", StringComparison.InvariantCultureIgnoreCase));

if (grpcWebOverHttp2)
    Console.WriteLine("GrpcWeb over http/2");
else if (grpcWeb)
    Console.WriteLine("GrpcWeb over http/1.1");

var grpcChannelOptions = new GrpcChannelOptions()
{
    HttpHandler = grpcWebOverHttp2
        ? new GrpcWebHandler(GrpcWebMode.GrpcWeb, new HttpClientHandler())
        : grpcWeb
            ? new GrpcWebHandler(GrpcWebMode.GrpcWeb, new Http11 { InnerHandler = new HttpClientHandler() })
            : new SocketsHttpHandler
            {
                PooledConnectionIdleTimeout = TimeSpan.FromSeconds(60),
                KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(20),
                EnableMultipleHttp2Connections = false
            },
};

var channel = GrpcChannel.ForAddress(new Uri("http://" + address), grpcChannelOptions);
var readerClient = new ReaderService.ReaderServiceClient(channel);
var writerClient = new WriterService.WriterServiceClient(channel);

Logger.StartStopwatch();
readerClient.ListChanges(new() { FromDatasetVersion = long.MaxValue }).Log("First Call");
var oldV = readerClient.ListChanges(new() { FromDatasetVersion = long.MaxValue }).Log("GetLatestVersion()").ToDatasetVersion;
readerClient.ListChanges(new() { FromDatasetVersion = oldV }).Log($"ListChanges({oldV}..)");
writerClient.Put(CreateDemodto()).Log("Put(Demodto)");
readerClient.ListChanges(new() { FromDatasetVersion = oldV }).Log($"ListChanges({oldV}..)");
int pageSize = 1;
var pt = readerClient.ListChanges(new() { PageSize = pageSize }).Log($"ListChanges(0.. | {pageSize})").NextPageToken;
while (pt.Length > 0)
{
    pageSize += pageSize;
    pt = readerClient.ListChanges(new() { PageSize = pageSize, PageToken = pt }).Log($"ListChanges(0.. | {pageSize})")
        .NextPageToken;
}

Demodto CreateDemodto()
{
    var contentId = Guid.NewGuid().ToString().ToLowerInvariant();
    return new Demodto
    {
        Name = $"vodproviders/TEST/vodpackages/TEST01/videofiles/cdcdcdcddeadbeef/demodtos/{contentId}",
        SomeString = contentId,
        SomeInt = 1800,
        SomeBool = true,
    };
}

namespace TestClient
{
    internal static class Logger
    {
        private static readonly Stopwatch StopWatch = new();
        private static long _lastElapsed = StopWatch.ElapsedMilliseconds;

        public static void StartStopwatch()
        {
            StopWatch.Start();
            _lastElapsed = StopWatch.ElapsedMilliseconds;
        }

        public static T Log<T>(this T xx, string who)
        {
            var elapsed = StopWatch.ElapsedMilliseconds;
            var duration = elapsed - _lastElapsed;
            Console.WriteLine($"{who} ({duration} ms):");
            Console.WriteLine(xx);
            Console.WriteLine();
            _lastElapsed = StopWatch.ElapsedMilliseconds;
            return xx;
        }
    }

    internal class Http11 : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            request.Version = Version.Parse("1.1");
            return base.SendAsync(request, cancellationToken);
        }
    }
}
