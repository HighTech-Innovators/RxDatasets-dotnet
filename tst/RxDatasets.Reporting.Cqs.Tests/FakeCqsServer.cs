using System.Collections.Concurrent;
using System.Net;

namespace RxDatasets.Reporting.Cqs.Tests;

[BindServiceMethod(typeof(FakeCqsServiceBase), nameof(BindService))]
public abstract class FakeCqsServiceBase
{
    public virtual Task<PushBatchResponse> PushBatch(PushBatchRequest request, ServerCallContext context) =>
        throw new RpcException(new Status(StatusCode.Unimplemented, "No handler configured."));

    public static void BindService(ServiceBinderBase binder, FakeCqsServiceBase? service)
    {
        Method<PushBatchRequest, PushBatchResponse> method = new(MethodType.Unary,
            "hti.rxdatasets.cqs.ingress.v1.CqsIngressPushService", "PushBatch",
            Marshallers.Create(request => request.ToByteArray(), PushBatchRequest.Parser.ParseFrom),
            Marshallers.Create(response => response.ToByteArray(), PushBatchResponse.Parser.ParseFrom));
        binder.AddMethod(method, service == null ? null : service.PushBatch);
    }
}

public sealed class FakeCqsService : FakeCqsServiceBase
{
    public ConcurrentQueue<PushBatchRequest> Requests { get; } = new();
    public ConcurrentQueue<(DateTime Deadline, DateTime Received)> Deadlines { get; } = new();

    public Func<PushBatchRequest, ServerCallContext, Task<PushBatchResponse>> Handler { get; set; }
        = (request, context) => Task.FromResult(new PushBatchResponse { Durable = true });

    public override Task<PushBatchResponse> PushBatch(PushBatchRequest request, ServerCallContext context)
    {
        Requests.Enqueue(request.Clone());
        Deadlines.Enqueue((context.Deadline, DateTime.UtcNow));
        return Handler(request, context);
    }
}

public sealed class FakeCqsServer : IAsyncDisposable
{
    private readonly WebApplication _application;

    private FakeCqsServer(WebApplication application, FakeCqsService service)
    {
        _application = application;
        Service = service;
        Endpoint = application.Urls.Single();
        Channel = GrpcChannel.ForAddress(Endpoint);
        Client = new CqsIngressPushService.CqsIngressPushServiceClient(Channel);
    }

    public FakeCqsService Service { get; }
    public string Endpoint { get; }
    public GrpcChannel Channel { get; }
    public CqsIngressPushService.CqsIngressPushServiceClient Client { get; }

    public static async Task<FakeCqsServer> Start()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0,
            listen => listen.Protocols = HttpProtocols.Http2));
        FakeCqsService service = new();
        builder.Services.AddSingleton(service);
        builder.Services.AddGrpc();
        WebApplication application = builder.Build();
        application.MapGrpcService<FakeCqsService>();
        await application.StartAsync();
        return new FakeCqsServer(application, service);
    }

    public async ValueTask DisposeAsync()
    {
        Channel.Dispose();
        await _application.DisposeAsync();
    }
}