# 10. The DataVlow server

The DataVlow server (repository `datavlowserver-dotnet`) is a ready-made gRPC server on RxDatasets that serves record
types **it has never seen at compile time**. You drop a `.proto` file in a folder, and within seconds the server
serves a reader, writer and history service for it. No code generation, no build, no deployment.

Use it when you want the standard [contracts](02-contracts.md) without writing a server: for development and tests,
as a reference implementation, or as a small production server where one instance is enough.

## How it differs from a hand-written server

| Hand-written server | DataVlow server |
|---|---|
| A C# class per record type from `protoc`, and `AddDtoServer<T>()` per type | No code generation. One CLR type per record type is emitted at runtime, only so RxDatasets has a type to work with. |
| Records are parsed into typed messages | **Bytes in, bytes out.** Validation and aliases work on the wire bytes against a runtime descriptor; nothing parses a record into an object. |
| A new type means a code change and a deployment | A new `.proto` file is picked up while the server runs |
| Services compiled per type | gRPC methods registered at runtime; reflection follows every change |
| All datasets in one service container | **One service container per dataset**, so a type can be added, replaced or removed while the host runs |

It shows that RxDatasets needs a type only as a name and a parser: `IDatasetReader<T>` and `IDatasetWriter<T>` work on
bytes and never use `T` in their members.

## Quick start

```bash
docker build --target final -t datavlow-server .
docker run --rm -p 10080:10080 -e Jwt__Disable=true \
  -v "$PWD/samples/protos:/datavlow/protos:ro" datavlow-server

grpcurl -plaintext localhost:10080 list
grpcurl -plaintext -d '{"dto":{"id":"t/1/l/central","external_id":"ams-01","name":"Central"}}' \
  localhost:10080 hti.rxdatasets.datavlow.library.v1.WriterService/Put
grpcurl -plaintext -d '{"id":"t/1/l/ams-01","alias_name":"by_externalid"}' \
  localhost:10080 hti.rxdatasets.datavlow.library.v1.ReaderService/Read
```

Without Docker:

```powershell
mkdir data\protos; copy samples\protos\*.proto data\protos\
$env:ASPNETCORE_ENVIRONMENT = "Development"        # turns JWT off and the change queue on
$env:LocalDiskStorage__RootFolder = "$PWD\data"
dotnet run --project src/DataVlow.Server.Host -c Release
```

## Defining a record type

One file per record type and major version: `<name>.v<N>.proto`, with `<name>` in lowercase letters and digits. The
file holds only the type's own definitions, without `syntax`, `package` or `option` lines; the server adds those from
its template.

```proto
message Library {
    // minor version 2

    // __idpattern__ t/{tenantid}/l/{libraryid}
    string id = 1;

    // __alias_name__ by_externalid __aliaspattern__ t/{tenantid}/l/{externalid} __aliasexpression__ t/{tenantid}/l/{.external_id}
    string external_id = 2;

    string name = 3;
}
```

- The top-level message is named after the file in PascalCase (`Library`, `Bookcopy`), and has `string id = 1`.
- `minor version <n>` is required, anywhere in a comment.
- `__idpattern__` gives the shape of the ids; there may be several. `{tenantid}` in it makes the type tenant-scoped.
- An alias is one line with `__alias_name__`, `__aliaspattern__` and `__aliasexpression__`. **A type gets no aliases
  at all unless it declares an `__aliaspattern__`.** In expressions, `{.field.sub}` reads a field, `items[]` walks a
  repeated field, `{.id/[..4]}` takes id segments, and `{name}` takes the id segment of that name. A write whose
  aliases don't match the pattern is rejected with `INVALID_ARGUMENT`.
- A type may import the template's shared protos by their path and refer to them relatively.

The `samples/protos` folder has an example of every feature:

| File | Shows |
|---|---|
| `library.v1` | A tenant type, an alias, an enum |
| `book.v1` | A child of a library; an alias per element of a repeated field |
| `bookcopy.v1` | Oneofs, several expressions per alias, id segments, an alias from a field that holds another record's id |
| `bookcopy.v2` | The next major version, served next to v1 in its own folder |
| `shelf.v1` | Two id patterns |
| `genre.v1` | A global type (no `{tenantid}`) |
| `loan.v1` | Aliases over an enum and map keys, a `google/protobuf` import |
| `reader.v1` | An import of the template's shared protos |

## Loading and hot reload

1. **Before the server accepts calls**, it reads every matching file in `ProtoSource:Prefix`. Files with other names
   are logged and skipped, and subfolders are ignored. A proto that doesn't compile stops the startup, and so does an
   empty folder. Every RPC answers `UNAVAILABLE` until its dataset is loaded.
2. **Then** the datasets load in the background, one by one. Each type is served as soon as its dataset is ready, and
   readiness turns healthy when all are.
3. **Every `ProtoSource:PollInterval`** (10 seconds), the server compares the files with what it serves, and adds,
   removes or changes types. A change to alias definitions rebuilds the dataset (RxDatasets recomputes the stored
   aliases); other accepted changes swap the descriptors of the running dataset.

**Unsafe changes are rejected**, and the previous version stays served: changing the id pattern or the id field,
changing alias patterns, removing or editing alias expressions, retyping a field an alias reads. The status service
shows the rejected change and why. An empty folder listing never removes the last type.

Each major version is its own dataset, in its own folder: `Library` for v1, `BookcopyV2` for v2.

## What it serves

Ports: **10080** for gRPC over cleartext HTTP/2, **18080** for HTTP/1.

| Service | Content |
|---|---|
| `{package}.{type}.v{N}.ReaderService` | `Read`, `BatchRead`, `ReadChildren`, `ListChildren`, `CountChildren`, `BatchLookup`, `BatchListChildren`, `BatchCountChildren`, and the deprecated `List` |
| `{package}.{type}.v{N}.WriterService` | `Put`, `PutMany`, `PutAll`, `PutAllChildren`, `PutOrDeleteMany`, `Delete`, `DeleteMany`, `DeleteChildren`, each with `sla_timestamp` and `tracking_id` |
| `{package}.{type}.v{N}.HistoryService` | `ReadHistoric`, `GetHistory`, `StreamHistory`, `TailHistory` |
| `{package}.testserver.DtoStatusService` | `ListDtos`: every proto file, whether it is served, its minor version, and its last rejected change |
| `{package}.DtoChangeQueueService` | With the change queue plugin on: `CreateOrConfigureQueue`, `Dequeue`, `BatchDequeue`, `Acknowledge`, `BatchAcknowledge`, `ReEnqueue`, `SelfEnqueue` |
| gRPC reflection, gRPC health | Health services `""`, `Startup`, `Readiness`, `Liveness` |
| `GET /` and `GET /migrate/{name}` | The second runs an RxDatasets [migration](08-dataset-lifecycle.md#data-migrations) on every dataset (authenticated) |

The writer stores a record without its `id` field (the id is the dataset name) and the reader puts it back, so
stored records are a little smaller.

Not supported: gRPC-Web, JSON transcoding, TLS, and role or scope checks.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `ProtoSource:Backend` | `LocalDisk` | Where the protos come from: `LocalDisk` or `GoogleCloud` (the `GoogleCloud` section, protos only) |
| `ProtoSource:Prefix` | `protos/` | The folder with the protos, under the storage root. Not the data root itself. |
| `ProtoSource:PollInterval` | `00:00:10` | How often to look for changed protos; 0 or less turns hot reload off |
| `LocalDiskStorage:RootFolder` | `data` (`/datavlow` in the image) | The data, and the protos with the `LocalDisk` backend |
| `Storage:CustomRootFolder` | `""` | A prefix in front of every dataset folder |
| `DtoStorageCache:CacheSizeMB` | `1024` | The cache, **per type** |
| `PageSize`, `MaxPageSize`, `PaginationCacheDuration` | `500`, `1000`, `60` | Reader paging (the code's fallback for `PageSize` is 1000) |
| `DATAVLOW:DTOS` | all | A comma-separated list of the type names to serve |
| `CHANGEQUEUESERVICE:ENABLED` | `false` | Turns the in-memory change queue on |
| `ChangeQueueService:DefaultSlaOffset` | `02:00:00` | The SLA of a write without one: now plus this |
| `CQS:DTOS` | all | The types that report into the change queue |
| `Jwt:Disable` | `false` | `true` lets every caller in; the tenant check is then a check of the id's shape only |
| `Jwt:Issuer`, `Jwt:Audiences`, `Jwt:JwksUrl` | | Required when JWT is on |
| `TenantAuth:ClaimNames` | `["tenant_id", "tenantId"]` | The claims that hold the tenant (an array) |
| `TenantAuth:PathPrefix` | `t` | The first segment of a tenant-scoped id |
| `TenantAuth:SuperTenantId` | none | A tenant that may access every tenant |
| `GoogleCloudLogging:Enabled` | `false` | Structured JSON logs for Google Cloud |
| `Contract:Organization`, `Contract:Product`, `Contract:Service` | `hti`, `rxdatasets`, `datavlow` | The names in packages and service names; see `docs/contract.md` |

The shipped `appsettings.json` has JWT on, so it doesn't start without JWT settings. `appsettings.Development.json`
turns JWT off and the change queue on.

**Tenancy:** a caller needs a tenant claim. The super tenant passes everything. Ids whose pattern has no `{tenantid}`
are global. Every other id must start with `t/{tenant}/` for the caller's own tenant. Health and reflection are open.

**Contract names:** the service and package names come from the `Contract:*` settings, so the server can match the
names an existing deployment's clients were generated with. Record protos are never rewritten.

## How it uses RxDatasets

Each record type gets its own service collection:

```csharp
var services = new ServiceCollection();
services.AddSingleton(root.GetRequiredService<IConfiguration>());
services.AddSingleton(root.GetRequiredService<ILoggerFactory>());

services.AddDtoServer<T>().WithCache().WithLocalDiskStorage().WithSharedReaderWriter().Build("readiness");
services.PostConfigure<StorageConfig>(c => c.DtoName = StorageFolder(typeof(T)));    // a folder per type
foreach (var alias in aliases)
    services.AddDatasetAlias<T>(alias.Name, alias.Factory, alias.DefinitionHash);
foreach (var plugin in plugins)
    plugin.ConfigureDtoServices<T>(services, dto);                                     // e.g. the change queue

return services.BuildServiceProvider();
```

- The server starts each collection's hosted services itself, and folds its health checks into the root's readiness.
- **No lock, no snapshots, no cleaning, local disk only.** Run **one instance** per data folder. All history is
  kept, and every start replays all deltas.
- Collections are built under a global lock, because RxDatasets' configuration validation isn't safe to run in
  parallel.
- Removing or replacing a type drains it: new calls are refused, in-flight writes finish (up to 30 seconds),
  reporters catch up to the newest version, the hosted services stop in reverse order, and the collection is
  disposed. Only then does a replacement start on the same folder.

## Plugins

A plugin adds protos to the template, services to every dataset's collection, and gRPC services to the server:

```csharp
public interface IDataVlowPlugin
{
    IEnumerable<KeyValuePair<string, string>> ProtoFiles => [];                  // added to the template
    void ConfigureDtoServices<T>(IServiceCollection services, DtoPluginContext dto)
        where T : class, IMessage<T>, new() { }                                // per dataset collection
    void MapServices(ContractServices services) { }                             // once
}
```

Register it as a singleton `IDataVlowPlugin` in the host. `ConfigureDtoServices` runs under the build lock, so it
only registers services and does no I/O. A reporter registered as
`DatasetUpdateConsumingService<T, DatasetUpdateReportingPlugin<T, TReporter>>` is drained before storage stops.

The in-memory change queue is the shipped plugin. Its queues live in the server's memory: per instance, lost on a
restart, with a two-minute lease and no dead letters. It is the right queue for tests and development, not for
production.

## Embedding it

The host is a few lines, so your own host can add plugins:

```csharp
var builder = WebApplication.CreateSlimBuilder(args);
DataVlow.Server.DI.ConfigureServices(builder.Services, builder.Configuration);
builder.Services.AddInMemoryChangeQueue(builder.Configuration);   // only when CHANGEQUEUESERVICE:ENABLED
builder.Services.AddSingleton<IDataVlowPlugin, MyPlugin>();
var app = builder.Build();
DataVlow.Server.DI.ConfigureApp(app);
app.Run();
```

The packages are `HighTechInnovators.DataVlow.Server`, `.Auth`, `.Contracts` (the shared request and response
messages) and `.ChangeQueue.InMemory`. The ports are set in the host's `Program.cs`.

## Clients

- Find the services with gRPC reflection (`grpcurl list`, `describe`), and the served types with
  `DtoStatusService/ListDtos`.
- Generate typed clients from the expanded protos: the template and the record proto under your contract names. The
  shared request and response types are in the `.Contracts` package.
- Retry `UNAVAILABLE` (a type is loading or reloading). `UNIMPLEMENTED` means the type isn't served.
- Send a bearer token with a tenant claim, unless JWT is off.
- Set `sla_timestamp` and `tracking_id` on every write.
- Follow changes with `HistoryService.TailHistory` (from the dataset itself, durable) or the change queue (in memory,
  per instance).

## Running the tests

```bash
dotnet test DataVlow.Server.slnx -c Release
./run-e2e.sh          # docker compose: the server and a grpcurl test runner
```

The end-to-end suite serves a few sample types with JWT off and the change queue on, and checks id validation,
aliases, streaming writes, paging and change notifications. `DATAVLOW_IMAGE=<image>` tests an image you built
before.
