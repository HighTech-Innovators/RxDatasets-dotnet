# 9. Real-world servers

RxDatasets runs in production in two families of servers, built in two different ways. This chapter shows how each
is put together, what they have in common, and what is worth copying. The names of record types are made generic.

- **The multi-type server** serves every record type of a system from one process: one dataset per type,
  generated from a template. A variant of it serves record types from raw protos at runtime, on cloud storage.
- **The per-type servers** are one small server per record type, each in its own container, all generated from one
  shared template.

## Generating a server per record type

Both families generate code from the same three inputs:

```
.template/      a git submodule with the code template (host, per-type services, Dockerfile)
.protosource/   a git submodule with the record type protos
dtos.lst        which protos to generate for, e.g. "order.v1.proto"
generated/      the output, never edited by hand
```

The template marks what repeats per type, and placeholders take the type's names:

```csharp
// FOREACH-DTO-START
services.AddDtoServer<Acme.Dtos.__DtoName____DtoVersion__.__DtoName__>()
    .WithCache()
    .WithLocalDiskStorage()
    .Build("readiness");
// FOREACH-DTO-END
```

A generator script expands the template for every proto in `dtos.lst` (`__DtoName__`, `__DtoVersion__`, and blocks
that repeat per id pattern or per alias), and a multi-stage Dockerfile runs it: generate, restore, build, test,
publish. Updating every server means bumping the template submodule in each server's repository; a script does that
for all of them. CI checks that the image name matches `dtos.lst`, which catches copy-paste mistakes between
repositories.

The [DataVlow server](10-datavlow-server.md) removes this step entirely: it reads the protos at runtime.

## Common shape

Both families have the same skeleton:

```csharp
var builder = WebApplication.CreateSlimBuilder(args);
builder.WebHost.ConfigureKestrel(o =>
{
    o.Listen(IPAddress.Any, 18080, l => l.Protocols = HttpProtocols.Http1);  // health checks, gRPC-Web
    o.Listen(IPAddress.Any, 10080, l => l.Protocols = HttpProtocols.Http2);  // gRPC, cleartext HTTP/2
});
Host.DI.ConfigureServices(builder.Services, builder.Configuration);
var app = builder.Build();
Host.DI.ConfigureApp(app);
app.Run();
```

- Two ports: HTTP/1 for health checks and gRPC-Web, HTTP/2 without TLS for gRPC. TLS ends at the load balancer or
  service mesh.
- Static `DI` classes per host and per type, so tests can build the same container.
- gRPC reflection on, so `grpcurl list` and `describe` work without proto files.
- Thin gRPC services over `GrpcReaderServiceImpl<T>` and `IDatasetWriter<T>`, which turn `InvalidOperationException`
  into `UNAVAILABLE` so clients retry while a dataset loads.
- The reader and writer resolved once after `Build()`, so every dataset starts loading at startup:

  ```csharp
  _ = app.Services.GetRequiredService<IDatasetReader<Order>>();
  _ = app.Services.GetRequiredService<IDatasetWriter<Order>>();
  ```

- The same sections and similar values: `SnapshotCreation`, `DtoStorageCache:CacheSizeMB = 1024`,
  `Cleanup:MinimumSnapshotsToKeep = 3`, `PageSize = 500`, `MaxPageSize = 1000`.
- Changes leave the server through a reporter.

## The multi-type server

One process, many datasets.

```csharp
// For every record type, inside the template's FOREACH block
if (IsTypeEnabled("Order", configuration))          // DATAVLOW__DTOS=order,customer ; empty = all
{
    OrderV1.Server.DI.ConfigureServices(services);   // the type's gRPC services and GrpcReaderServiceImpl
    services.AddDtoServer<Acme.Dtos.OrderV1.Order>()
        .WithCache()
        .WithLocalDiskStorage()
        .WithSharedReaderWriter()
        .Build("readiness");

    if (IsChangeQueueEnabled(configuration))
        services.AddDtoChangeQueueReporter<Acme.Dtos.OrderV1.Order>();
}
```

What it shows:

- **A subset per deployment.** One image serves every type; a configuration list picks the types to serve. End-to-end
  tests serve only the types they test, which keeps startup fast.
- **Shared reader/writer.** A read sees a write, and its aliases, as soon as the write returns. Tests that write and
  then read need no retries.
- **Aliases from the protos.** The template turns each alias annotation into an `AddDatasetAliases<T>` call, and the
  writer service validates aliases before writing.
- **Concurrent start and stop** (`ServicesStartConcurrently`, `ServicesStopConcurrently`), so many datasets start in
  parallel.
- **A change queue in process.** A reporter per type enqueues every change into a queue service hosted in the same
  process (`CreateOrConfigureQueue`, `BatchDequeue`, `BatchAcknowledge`, `ReEnqueue`, `SelfEnqueue`), ordered by SLA.
- **History** served from the dataset: `GetHistory`, `StreamHistory`, `TailHistory`.
- **JWT authentication**, with the tenant checked per id and each id checked against the type's id pattern, also for
  every item of a streaming write. A configuration switch turns authentication off for local runs and tests, and
  logs a warning when it is off.
- **REST next to gRPC**, with JSON transcoding and a Swagger page, for tools and browsers.
- **Health probes per purpose:** gRPC health services `Startup`, `Readiness` and `Liveness`, keyed by the
  `readiness` tag. (It maps liveness to the untagged checks, which fail while a dataset loads; see
  [Health checks](04-building-a-server.md#health-checks) for the safer mapping.)

It runs as one serverless container with one instance, a long request timeout for streams, and
`DOTNET_GCHeapHardLimitPercent=75` against a small memory limit. On local disk without a lock it is a test server:
the data lives as long as the container.

### The variant on cloud storage

A later variant serves record types from raw protos at runtime (like the [DataVlow server](10-datavlow-server.md))
and stores data in a bucket. Its dataset setup is the clearest production recipe:

```csharp
var builder = services.AddDtoServer<T>().WithCache();
if (bucketConfigured)
{
    // The platform overlaps the old and the new instance on a deployment. The lock makes the new one ask for a
    // handover and wait for it before it reads or writes.
    builder.WithGoogleCloudStorage().WithDistributedLock();
    if (configuration["Storage:CustomRootFolder"] is null)
        services.PostConfigure<StorageConfig>(c => c.CustomRootFolder = "data/");   // apart from the protos
}
else
{
    builder.WithLocalDiskStorage();
}
builder.WithSnapshot().WithSharedReaderWriter().Build("readiness");   // no cleanup: keep all history
```

with `Writer:EnableWriteCoalescing = true` (many small writes on high-latency storage), a one-minute
`DistributedLock:RenewalInterval`, and a 168-hour snapshot period. On shutdown it waits until its change queue
reporter has reported up to the newest version before it stops storage, so no accepted write goes unreported when
the lock moves. Datasets with many types are a good fit for the [shared lock](07-locking-and-deployment.md#one-lock-for-all-datasets-of-an-instance).

## The per-type servers

One container per record type, about twenty of them, all from one template.

```csharp
if (devMode)
    services.AddDtoServer<Acme.Dtos.OrderV1.Order>()
        .WithCache()
        .WithLocalDiskStorage()
        .Build();
else
    services.AddDtoServer<Acme.Dtos.OrderV1.Order>()
        .WithCache()
        .WithLocalDiskStorage()          // a network share
        .WithDistributedLock()           // active/passive hosts share the folder
        .WithSnapshot()
        .WithBusReporting()              // a builder extension of their own
        .WithCleanup()
        .Build();
```

What it shows:

- **A light development mode.** The same storage, without lock, snapshots, reporting and cleaning, so a developer runs
  a server with nothing but a folder.
- **A builder extension for its reporter.** A message per change goes to a message bus; the reporter throws when the
  broker doesn't confirm, so the delta is retried. See [Reporters](08-dataset-lifecycle.md#reporters).
- **One configuration for all.** Every server reads the same central configuration; only the folder differs per
  server, set by one environment variable (`STORAGE__DTONAME=order.v1`).
- **Active/passive hosts.** Every server runs as a system service on two hosts that mount the same network share, one
  folder per type. A flag decides which host runs them; the lock protects the data if both ever run at once.
- **A read-only front.** A gRPC proxy in front of all servers lets only the reader services through, so the public
  surface is read-only and writers stay internal.
- **Consumers poll the change feed.** Downstream services call `ListChanges` from their stored version, page to the
  end, store the new version, and wait a few seconds when nothing changed.
- **An in-memory test server** hosts every type in one process with `WithInMemoryDataset()`, optionally loaded from a
  start-state file. Other teams use it in their tests. Its versions restart at 1 on every start, so a client that
  stored a version must reset too.
- **Workstation garbage collection** (`ServerGarbageCollection=false`), because many small servers share each host.

## Side by side

| | Multi-type server | Per-type servers |
|---|---|---|
| Process model | All types in one process; a subset by configuration | One container per type |
| Storage | Local disk (test server); a bucket in the variant | Local disk on a network share |
| Lock | None (one instance); per dataset in the bucket variant | Per dataset in production, none in development |
| Snapshots and cleaning | Snapshots in the variant; no cleaning, all history kept | Both in production |
| Changes out | In-process change queue, history streams | A message per change on a bus; `ListChanges` polling |
| Health | gRPC health: startup, readiness, liveness | One HTTP `/healthz` |
| Auth | JWT, tenant per id, id patterns | None in the server; a read-only proxy in front |
| Deployment | One serverless container, overlapping on deploy | System services on two hosts, active/passive |
| Configuration | `appsettings.{Environment}.json` and environment variables | A central configuration service, one environment variable per server |

## Lessons

1. **Resolve the reader and writer at startup.** Both families do it; without it a dataset only starts loading on the
   first request.
2. **Turn "not ready" into `UNAVAILABLE`** and let clients retry. During startup, handovers and shutdown, it is the
   normal answer.
3. **Keep readiness and liveness apart.** An instance waiting for the lock is alive.
4. **Lock whenever storage is shared or deployments overlap.**
5. **One reporter identifier per storage location**, reporters registered after storage, and reporters that throw on
   failure.
6. **Never recreate a dataset under live clients.** Versions start at 1 again, and every stored cursor is wrong.
7. **Concurrent start and stop** for hosts with many datasets.
8. **Keep credentials out of shared configuration.** One family's central configuration held storage keys and broker
   credentials in plain text next to ordinary settings. Use a secret store.
9. **Keep up with the library.** One family pinned a version from long before tracking ids, SLAs and the
   compare-and-swap lock existed; its reporter interface no longer matches the current one.
