# 4. Building a server

This chapter builds a server for one record type, `Order`, from proto to a running gRPC service. Everything here is
also in the repository's sample apps: `testapps/DTOs` (protos and services), `testapps/Host` (a read/write host) and
`testapps/ReadOnlyHost` (a read-only replica).

## The packages

All packages target .NET 10. The ids on NuGet start with `HighTechInnovators.`.

| Package | Use it for |
|---|---|
| `HighTechInnovators.RxDatasets` | The engine: `AddDtoServer<T>()`, readers, writers, snapshots, cleaning, locks, reporters, health checks, `GrpcReaderServiceImpl<T>`. Always needed. |
| `HighTechInnovators.RxDatasets.Abstractions` | The interfaces (`IDatasetReader<T>`, `IDatasetWriter<T>`, `IDiskStorage<T>`, `IDatasetUpdateReporting<T>`, ...). Comes with the engine; reference it alone from libraries that only need the interfaces. |
| `HighTechInnovators.RxDatasets.StorageProvider.LocalDisk` | `WithLocalDiskStorage()` |
| `HighTechInnovators.RxDatasets.StorageProvider.GoogleCloud` | `WithGoogleCloudStorage()` |
| `HighTechInnovators.RxDatasets.StorageProvider.S3` | `WithS3Storage()`, also for S3-compatible stores such as MinIO |
| `HighTechInnovators.RxDatasets.StorageProvider.AzBlob` | `WithAzureStorage()` |
| `HighTechInnovators.RxDatasets.StorageProvider.Minio` | `WithMinioStorage()`, see the [caveat](06-storage-providers.md#minio) |
| `HighTechInnovators.RxDatasets.Aliasing` | `AddDatasetAliases<T>()`, alias readers |
| `HighTechInnovators.RxDatasets.Reporting` | `WithJsonReporting()`: every change as a JSON file |
| `HighTechInnovators.RxDatasets.DatasetWatcher` | `IDatasetWatcher<T>`: replay a dataset and follow it, straight from storage |
| `HighTechInnovators.RxDatasets.InMemoryDataset` | `InMemoryDataset<T>` for unit tests |

The library ships no gRPC services and no client. You define the services in your own protos, and implement them on
top of `GrpcReaderServiceImpl<T>` and `IDatasetWriter<T>`, as below. If you want the standard DataVlow contract
without writing any code, use the [DataVlow server](10-datavlow-server.md).

## Step 1: the record type

```proto
syntax = "proto3";
package acme.dtos.order.v1;
option csharp_namespace = "Acme.Dtos.OrderV1";

message Order {
  // minor version 1
  // __idpattern__ t/{tenantid}/s/{siteid}/orders/{orderid}
  string id = 1;
  string customer_id = 2;
  repeated OrderLine lines = 3;
}

message OrderLine {
  string sku = 1;
  int32 quantity = 2;
}
```

RxDatasets needs `T : class, IMessage<T>`, which every generated protobuf class is. It stores each record as its
serialized bytes under a string name; you choose which field is the name. Here it is `id`.

The dataset's storage folder is named after the type: `Order` here. When the C# namespace ends in a version other
than 1 (`Acme.Dtos.OrderV2`), the version is appended (`OrderV2`), so two major versions never share a folder. Set
`Storage:DtoName` to choose another name.

## Step 2: the service protos

A reader and a writer service. Add whichever operations you need from [Service contracts](02-contracts.md); this one
is kept short.

```proto
syntax = "proto3";
package acme.dtos.order.v1;
option csharp_namespace = "Acme.Dtos.OrderV1";

import "google/protobuf/timestamp.proto";
import "acme/dtos/order/v1/order.proto";

service ReaderService {
  rpc Read(ReadOrderRequest) returns (Order);
  rpc BatchRead(BatchReadOrdersRequest) returns (BatchReadOrdersResponse);
  rpc ReadChildren(ReadChildrenRequest) returns (ReadChildrenResponse);
  rpc ListChanges(ListChangesRequest) returns (ListChangesResponse);
}

service WriterService {
  rpc Put(PutOrderRequest) returns (WriteResponse);
  rpc PutMany(stream PutOrderRequest) returns (WriteResponse);
  rpc Delete(DeleteOrderRequest) returns (WriteResponse);
}

message ReadOrderRequest { string id = 1; }
message BatchReadOrdersRequest { repeated string ids = 1; string parent = 2; }
message BatchReadOrdersResponse { repeated Order orders = 1; }
message ReadChildrenRequest { string parent = 1; int32 page_size = 2; string page_token = 3; }
message ReadChildrenResponse { repeated Order orders = 1; string next_page_token = 2; }

message ListChangesRequest {
  uint64 from_dataset_version = 1;
  int32 page_size = 2;
  string page_token = 3;
  bool wait_for_changes = 4;
}
message ListChangesResponse {
  uint64 to_dataset_version = 1;
  repeated string new_entities = 2;
  repeated string changed_entities = 3;
  repeated string deleted_entities = 4;
  string next_page_token = 5;
}

message PutOrderRequest {
  Order dto = 1;
  google.protobuf.Timestamp sla_timestamp = 2;
  string tracking_id = 3;
}
message DeleteOrderRequest {
  string id = 1;
  google.protobuf.Timestamp sla_timestamp = 2;
  string tracking_id = 3;
}
message WriteResponse {
  uint64 new_dataset_version = 1;
  int32 num_created = 2;
  int32 num_updated = 3;
  int32 num_deleted = 4;
}
```

Compile them with `Grpc.Tools` in the project file:

```xml
<ItemGroup>
  <Protobuf Include="protos/**/*.proto" ProtoRoot="protos" GrpcServices="Server" />
</ItemGroup>
```

## Step 3: the services

`GrpcReaderServiceImpl<T>` does the reading: it parses records, pages, resolves aliases and turns errors into
`RpcException`s (`NOT_FOUND`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION` for a bad page token, `UNAVAILABLE` while the
host is stopping). Register it as a singleton and wrap it:

```csharp
public class OrderReader(GrpcReaderServiceImpl<Order> reader) : ReaderService.ReaderServiceBase
{
    public override Task<Order> Read(ReadOrderRequest request, ServerCallContext context) =>
        reader.Get(request.Id, context.CancellationToken);

    public override async Task<BatchReadOrdersResponse> BatchRead(BatchReadOrdersRequest request, ServerCallContext context) =>
        new() { Orders = { await reader.BatchGet(request.Ids, request.Parent, context.CancellationToken) } };

    public override async Task<ReadChildrenResponse> ReadChildren(ReadChildrenRequest request, ServerCallContext context)
    {
        var (orders, nextPageToken) = await reader.GetChildren(request.Parent, request.PageSize, request.PageToken,
            context.CancellationToken);
        return new() { Orders = { orders }, NextPageToken = nextPageToken ?? "" };
    }

    public override async Task<ListChangesResponse> ListChanges(ListChangesRequest request, ServerCallContext context)
    {
        var changes = await reader.ListChanges(new(request.FromDatasetVersion, request.PageSize, request.PageToken,
            request.WaitForChanges), context.CancellationToken);
        return new()
        {
            ToDatasetVersion = changes.ToDatasetVersion,
            NewEntities = { changes.NewEntities },
            ChangedEntities = { changes.ChangedEntities },
            DeletedEntities = { changes.DeletedEntities },
            NextPageToken = changes.NextPageToken,
        };
    }
}
```

The writer goes straight to `IDatasetWriter<T>`. Every write method takes a tracking id and an SLA timestamp per
record; the older overloads without them are obsolete.

```csharp
public class OrderWriter(IDatasetWriter<Order> writer) : WriterService.WriterServiceBase
{
    public override async Task<WriteResponse> Put(PutOrderRequest request, ServerCallContext context) =>
        Map(await writer.Put(request.Dto.Id, request.Dto.ToByteArray(), Tracking(request.TrackingId),
            request.SlaTimestamp?.ToDateTimeOffset(), context.CancellationToken));

    public override async Task<WriteResponse> PutMany(IAsyncStreamReader<PutOrderRequest> requests, ServerCallContext context)
    {
        // Streamed straight into the dataset: the whole call becomes one version.
        var stream = requests.ReadAllAsync(context.CancellationToken).Select(r =>
            (r.Dto.Id, r.Dto.ToByteArray(), Tracking(r.TrackingId), r.SlaTimestamp?.ToDateTimeOffset()));
        return Map(await writer.PutMany(stream, context.CancellationToken));
    }

    public override async Task<WriteResponse> Delete(DeleteOrderRequest request, ServerCallContext context) =>
        Map(await writer.Delete(request.Id, Tracking(request.TrackingId), request.SlaTimestamp?.ToDateTimeOffset(),
            context.CancellationToken));

    private static string? Tracking(string id) => id.Length == 0 ? null : id;

    private static WriteResponse Map(IDatasetWriter<Order>.WriteResponse r) => new()
    {
        NewDatasetVersion = r.NewDatasetVersion,
        NumCreated = (int)r.NumAddedEntities,
        NumUpdated = (int)r.NumUpdatedEntities,
        NumDeleted = (int)r.NumDeletedEntities,
    };
}
```

A real writer also checks every id against the type's id pattern and the caller's tenant before it writes; see
[Security](02-contracts.md#security-and-tenant-isolation).

### Map exceptions in one place

RxDatasets reports some conditions as plain .NET exceptions. Map them once, in a gRPC interceptor, instead of a
try/catch in every method:

| Exception | Meaning | Status |
|---|---|---|
| `InvalidOperationException` | The dataset isn't initialized yet, storage isn't ready, or this instance doesn't hold the lock | `UNAVAILABLE` (the client retries) |
| `AliasConflictException` | Another record already has one of this record's aliases; nothing was stored | `ALREADY_EXISTS` |
| `ArgumentException`, `ArgumentOutOfRangeException` | An empty or disallowed name, a bad `DeleteChildren` ancestor, or a record whose aliases can't be computed | `INVALID_ARGUMENT` |
| `System.Data.DBConcurrencyException` | Another writer committed this version first; the dataset reloads | `ABORTED` (the client retries) |

```csharp
public class DatasetExceptionInterceptor : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request,
        ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try { return await continuation(request, context); }
        catch (Exception e) when (Map(e) is { } rpc) { throw rpc; }
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try { return await continuation(requestStream, context); }
        catch (Exception e) when (Map(e) is { } rpc) { throw rpc; }
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(TRequest request,
        IServerStreamWriter<TResponse> responseStream, ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try { await continuation(request, responseStream, context); }
        catch (Exception e) when (Map(e) is { } rpc) { throw rpc; }
    }

    private static RpcException? Map(Exception e) => e switch
    {
        RpcException => null,
        AliasConflictException => new(new Status(StatusCode.AlreadyExists, e.Message)),
        InvalidOperationException => new(new Status(StatusCode.Unavailable, e.Message)),
        ArgumentException => new(new Status(StatusCode.InvalidArgument, e.Message)),
        DBConcurrencyException => new(new Status(StatusCode.Aborted, e.Message)),
        _ => null,
    };
}
```

```csharp
services.AddGrpc(o => o.Interceptors.Add<DatasetExceptionInterceptor>());
```

`InvalidOperationException` must go first after `RpcException`, because `ObjectDisposedException` is one too: a
dataset that is shutting down is unavailable, not broken.

## Step 4: the host

```csharp
using System.Net;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using RxDatasets;
using RxDatasets.Abstractions;
using RxDatasets.StorageProvider.LocalDisk;

var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.ConfigureKestrel(o =>
{
    o.Listen(IPAddress.Any, 18080, l => l.Protocols = HttpProtocols.Http1);  // health checks, gRPC-Web
    o.Listen(IPAddress.Any, 10080, l => l.Protocols = HttpProtocols.Http2);  // gRPC over cleartext HTTP/2
});

var services = builder.Services;
services.AddGrpc(o => o.Interceptors.Add<DatasetExceptionInterceptor>());
services.AddGrpcReflection();                       // lets grpcurl list and describe the services
services.AddSingleton<GrpcReaderServiceImpl<Order>>();

services.AddDtoServer<Order>()
    .WithLocalDiskStorage()
    .WithCache()
    .WithSnapshot()
    .WithSharedReaderWriter()
    .Build("readiness");                            // tag the readiness checks

var app = builder.Build();
app.MapGrpcService<OrderReader>();
app.MapGrpcService<OrderWriter>();
app.MapGrpcReflectionService();
app.MapHealthChecks("/healthz", new HealthCheckOptions { Predicate = c => !c.Tags.Contains("readiness") });
app.MapHealthChecks("/readiness", new HealthCheckOptions { Predicate = c => c.Tags.Contains("readiness") });

// The reader and writer are created on first use, and only start loading the dataset then. Create them now.
app.Services.GetRequiredService<IDatasetReader<Order>>();
app.Services.GetRequiredService<IDatasetWriter<Order>>();

app.Run();
```

`appsettings.json`:

```json
{
  "LocalDiskStorage": { "RootFolder": "data" },
  "SnapshotCreation": { "SnapshotInterval": 100, "PeriodInHours": 24 },
  "DtoStorageCache": { "CacheSizeMB": 256 }
}
```

Run it and try it:

```bash
dotnet run
grpcurl -plaintext localhost:10080 list
grpcurl -plaintext -d '{"dto":{"id":"t/acme/s/site01/orders/o-1","customer_id":"c-7"},"tracking_id":"try-1"}' \
  localhost:10080 acme.dtos.order.v1.WriterService/Put
grpcurl -plaintext -d '{"id":"t/acme/s/site01/orders/o-1"}' localhost:10080 acme.dtos.order.v1.ReaderService/Read
```

### Why the reader and writer are resolved by hand

`IDatasetReader<T>` and `IDatasetWriter<T>` are singletons, but not hosted services. They are created on first use,
subscribe to storage readiness in their constructor, and only then load the dataset. Until they have loaded, every
call throws `InvalidOperationException("Initialization is not yet completed")`. Without the two lines before
`app.Run()`, loading starts only when the first readiness probe or request resolves them, so it starts later than it
could, and the first requests fail. Resolving them up front starts the load as soon as storage is ready. (A reader or
writer created later still learns that storage is ready: the notifications hub replays the last notification.)

### What the builder does

`AddDtoServer<T>()` starts a builder; `Build(...)` registers everything. In between you choose:

| Method | What it adds |
|---|---|
| `With...Storage()` | Where the dataset is stored. Exactly one is required (or `WithInMemoryDataset()`). See [Storage providers](06-storage-providers.md). |
| `WithCache()` | An in-memory cache of index files and record bytes in front of storage, `DtoStorageCache:CacheSizeMB` large. Use it on every storage except, perhaps, local disk. |
| `WithSnapshot()` | Writes a snapshot every `SnapshotCreation:SnapshotInterval` versions or `PeriodInHours` hours. Without snapshots, every start replays all deltas. Use it in production. |
| `WithCleanup()` | Deletes old snapshots and, optionally, old deltas. Needs `WithSnapshot()`, and only deletes once a reporter has stored its progress; see [Cleaning](08-dataset-lifecycle.md#cleaning). |
| `WithDistributedLock()` | A lock of this dataset's own, so only one instance writes it. See [Locking](07-locking-and-deployment.md). |
| `WithSharedDistributedLock()` | Puts the dataset under the instance's shared lock: one lock for all datasets. Needs `AddSharedDistributedLock<TStorage>()`. Can't be combined with `WithDistributedLock()`. |
| `WithSharedReaderWriter()` | One object, one index for reading and writing. A read sees a write as soon as the write returns, including its aliases. Saves memory. Best when writes are not very frequent. |
| `WithWriteCoalescing()` | Merges writes that arrive at the same time into one version. Helps on high-latency storage with many small concurrent writes. Applies to every dataset in the service collection, and not to `PutAll` and `PutAllChildren`. |
| `WithInMemoryDataset()` | Keeps the dataset in memory only, with the real reader and writer. For tests and development; ignores lock, snapshot and cleanup. |
| `Build(params string[] readinessTags)` | Registers everything. The tags go on the dataset's readiness check and on the lock check. |

`AddReadOnlyDtoServer<T>()` builds a read-only replica: a reader that follows storage, without a writer or lock. See
[Read-only replicas](07-locking-and-deployment.md#read-only-replicas).

### Health checks

`Build` registers per dataset (the name has the type and, for a major version, its suffix):

| Check | Tagged | Healthy when |
|---|---|---|
| `DatasetReadinessCheck-{type}` | yes | The reader and writer are initialized and storage answers |
| `DatasetHealthCheck-{type}` | no | The same, but "not started yet" counts as healthy. **Unhealthy while loading.** |
| `DistributedLockHealthCheck-{type}` | yes | This instance holds the lock (only with a lock) |
| `storage.googlecloud.{type}`, `storage.s3.{type}`, `storage.azblob.{type}`, `storage.minio.{type}` | `googlecloud` or `s3` and the type; Azure and MinIO none | The provider's last call worked, or a probe does. Local disk has none. |

Map the tagged checks to readiness. A dataset that is loading a large snapshot, or a standby waiting for the lock,
is alive but not ready, and the platform must not kill it. The untagged `DatasetHealthCheck` is no good for that: it
is unhealthy while a dataset loads, so a standby that takes over the lock and needs a while to load would be killed.
Let liveness and startup check that the process answers, and keep the dataset checks for readiness and diagnostics.
With gRPC health probes:

```csharp
services.AddGrpcHealthChecks(o =>
{
    o.Services.Map("", _ => true);                                // everything, for diagnostics
    o.Services.Map("Startup", _ => false);                        // the process answers
    o.Services.Map("Readiness", r => r.Tags.Contains("readiness"));
    o.Services.Map("Liveness", _ => false);                       // the process answers
});
app.MapGrpcHealthChecksService();
```

## More than one record type

Call `AddDtoServer<T>()` once per type. Every dataset gets its own folder, reader, writer and health checks.

Configuration sections are **per service collection, not per dataset**: all datasets in one collection share
`Storage`, `Writer`, `SnapshotCreation`, `Cleanup`, `DtoStorageCache`, `DistributedLock` and the provider's section.
So in a multi-type host, don't set `Storage:DtoName` (it would put every dataset in the same folder), and note that
the cache size applies to each dataset. A host that needs different settings per dataset builds a service
collection per dataset; the [DataVlow server](10-datavlow-server.md) does that.

With many datasets, start and stop the hosted services concurrently, or startup time grows with every type:

```csharp
services.Configure<HostOptions>(o =>
{
    o.ServicesStartConcurrently = true;
    o.ServicesStopConcurrently = true;
});
```

## Using a dataset in process

Anything in the host can use the dataset directly:

```csharp
public class OrderTotals(IDatasetReader<Order> orders)
{
    public async Task<int> CountLines(string siteId, CancellationToken ct)
    {
        var (names, _) = await orders.List($"t/acme/s/{siteId}", ct);
        var records = await orders.BatchGet(names, parent: null);
        return records.Where(r => r.entityBytes is not null)
            .Sum(r => Order.Parser.ParseFrom(r.entityBytes!.Value.Span).Lines.Count);
    }
}
```

`IDatasetReader<T>` works on bytes: `Get`, `BatchGet` (a missing record comes back with null bytes), `List`,
`CountChildren`, `GetChangedNames` and `WaitChangedNames` (the change feed behind `ListChanges`), and the history
methods `GetHistory`, `StreamHistory`, `TailHistory` and `GetHistoric`. To react to every change inside the host,
write a reporter; see [Reporters](08-dataset-lifecycle.md#reporters).

## Next steps

- [Configuration reference](05-configuration.md)
- [Storage providers](06-storage-providers.md)
- [Locking, replicas and deployment](07-locking-and-deployment.md)
- [Inside a dataset](08-dataset-lifecycle.md): snapshots, cleaning, reporters and aliases
