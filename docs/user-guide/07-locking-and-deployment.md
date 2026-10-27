# 7. Locking, replicas and deployment

A dataset has one writer. When more than one process can reach the same storage, something must decide which one
writes. That is the distributed lock. This chapter tells you when you need it, which kind to use, and how it fits
the way you deploy. The mechanism itself, with a timeline for every scenario, is in
[Distributed lock and fenced writes](../distributed-lock.md).

## Do you need a lock?

**Use a lock whenever two read/write processes can see the same storage, even for a moment.** That includes the
overlap of a rolling deployment, where the platform starts the new instance before it stops the old one.

| Situation | Lock |
|---|---|
| One instance, storage only it can reach (its own disk, its own folder) | Not needed |
| Rolling deployments on a bucket (Cloud Run, Kubernetes, App Service, ...) | Yes |
| Active/passive hosts on a shared disk | Yes |
| Several replicas that should all write | Not possible: one holds the lock and the others wait. Scale reads with read-only replicas. |
| Development, tests | No |

Without a lock, two writers on one dataset aren't safe. The [fenced delta commit](../distributed-lock.md#fenced-delta-commit)
keeps the files consistent on providers with compare-and-swap: the second writer of a version gets a
`DBConcurrencyException` and reloads. But both instances keep accepting writes, run snapshots and cleaning, and
report changes, so writes fail, and reporters can report a change twice or miss one.

## What the lock does to an instance

The instance that holds the lock (the **holder**) is the only one whose dataset is ready. On every other read/write
instance (a **standby**):

- the dataset isn't loaded, and **reads and writes both fail** with `UNAVAILABLE`;
- the readiness check `DistributedLockHealthCheck-{type}` is unhealthy, so a load balancer sends no traffic there;
- snapshots, cleaning and reporters don't run.

When a new instance starts, it writes a handover request. The holder sees it within one `PollingInterval`, stops
writing and releases the lock, and the new instance takes it over. With the defaults the handover takes at most about
10 seconds, plus the time the new instance needs to load the dataset. Requests that arrive at the old instance after
it released fail with `UNAVAILABLE`, so clients must retry.

If the holder crashes, its lock stays until it expires: up to `DistributedLock:RenewalInterval` (5 minutes by
default). Lower it if a standby must take over sooner after a crash.

Keep the clocks of all instances synchronized (NTP). Expiry is compared with each instance's own clock.

## One lock per dataset

```csharp
services.AddDtoServer<Order>().WithGoogleCloudStorage().WithDistributedLock().WithCache().WithSnapshot()
    .Build("readiness");
```

The lock's files (`lock-file`, `request-lock`) are in the dataset's folder. Each dataset is handed over on its own,
so during a deployment some datasets may be written by the old instance and some by the new one, for a few seconds.

## One lock for all datasets of an instance

A server with many datasets usually wants them to move together: one handover, one lock loop, one set of lock files.

```csharp
// Register the lock before the datasets.
services.AddSharedDistributedLock<GoogleCloudStorage<SharedDistributedLock>>("OrderServerLock");

services.AddDtoServer<Order>().WithGoogleCloudStorage().WithSharedDistributedLock().Build("readiness");
services.AddDtoServer<Customer>().WithGoogleCloudStorage().WithSharedDistributedLock().Build("readiness");
```

- The type argument is the storage the lock uses, closed over `SharedDistributedLock`:
  `GoogleCloudStorage<SharedDistributedLock>`, `S3Storage<SharedDistributedLock>`, `AzStorage<SharedDistributedLock>`,
  `LocalDiskStorage<SharedDistributedLock>`. Only storage with compare-and-swap compiles.
- The lock's files are in `{Storage:CustomRootFolder}{lockFolder}/`, read from the same provider section as the
  datasets. The lock folder names the kind of server: instances that share it hand over to each other. Give every kind
  of server its own, and never the name of a dataset folder.
- On stop, the lock waits until every dataset has flushed and stopped, and only then releases. While it waits, it
  doesn't hand over to a new instance, so the next holder sees all writes.
- **Registration order matters when the host stops services one by one** (the .NET default): register the lock
  before the datasets, so it stops after them. With `HostOptions.ServicesStopConcurrently = true` the order doesn't
  matter.
- `WithDistributedLock()` and `WithSharedDistributedLock()` on one dataset throws at startup.
- No health check watches the lock's storage by default. Add one if you want it:
  `services.AddHealthChecks().AddCheck<GoogleCloudHealthCheck<SharedDistributedLock>>("storage.googlecloud.SharedLock")`.

A host that builds a service collection per dataset registers the lock in its root collection, and adds the root's
instance to each dataset's collection:

```csharp
// Root
root.AddValidatedConfigSection<GoogleCloudConfig>();
root.AddSharedDistributedLock<GoogleCloudStorage<SharedDistributedLock>>("OrderServerLock");

// Per dataset: the root's instance, so this collection doesn't start, stop or dispose it
services.AddSingleton(rootProvider.GetRequiredService<SharedDistributedLock>());
services.AddDtoServer<T>().WithGoogleCloudStorage().WithSharedDistributedLock().Build("readiness");
```

## Read-only replicas

A read-only replica serves reads from the same storage as the writer, without a writer and without a lock:

```csharp
services.AddReadOnlyDtoServer<Order>().WithGoogleCloudStorage().WithCache().Build();
```

- It checks storage for new versions every 5 seconds, so it **lags the writer by about 5 to 10 seconds**. Don't use a
  replica where a client reads its own write right away.
- It is ready as soon as it has loaded, whether or not a writer runs.
- Page tokens live in one instance's memory. Behind a load balancer, either route all pages of a listing to the same
  replica, or accept that a token can fail with `FAILED_PRECONDITION` and restart the listing.
- Don't call `WithSnapshot()`, `WithCleanup()` or a lock on a read-only builder. Snapshots and cleaning belong to the
  writer.
- A read-only host serves the reader and history services only. Map no writer service on it.

## Deployment patterns

### A single instance on its own disk

The simplest setup: one instance, local disk on a persistent volume, no lock. Deploy with "stop, then start"
(Kubernetes `strategy: Recreate`), so two instances never share the volume. Good for test servers and for small
systems that tolerate a short outage per deployment.

### Rolling deployments on a bucket

One logical writer on cloud storage, with the platform's normal rolling deployments:

```csharp
services.AddDtoServer<Order>()
    .WithGoogleCloudStorage()
    .WithDistributedLock()          // or WithSharedDistributedLock() for many datasets
    .WithCache()
    .WithSnapshot()
    .WithSharedReaderWriter()
    .Build("readiness");
```

During a deployment the new instance asks for a handover and stays not-ready until it holds the lock and has loaded
the dataset. Traffic stays on the old instance until then. Configure:

- a **readiness** probe on the tagged checks (`Readiness`), and a **liveness** probe that excludes them;
- a **startup** probe long enough for the largest dataset to load from its latest snapshot;
- a termination grace period longer than the time the dataset needs to flush and the reporters to catch up;
- `HostOptions.ServicesStopConcurrently = true` with many datasets;
- one instance (no autoscaling of the writer), and a request timeout long enough for long-lived streams
  (`TailHistory` with `follow`, `ListChanges` with waiting).

```yaml
startupProbe:   { grpc: { port: 10080, service: Startup },   periodSeconds: 5, failureThreshold: 60 }
readinessProbe: { grpc: { port: 10080, service: Readiness }, periodSeconds: 5, failureThreshold: 1 }
livenessProbe:  { grpc: { port: 10080, service: Liveness },  periodSeconds: 10, failureThreshold: 3 }
```

### Active/passive hosts on a shared disk

Two hosts mount the same network folder. Normally one runs and the other is idle; the lock protects the data when
both run at once, during a failover or a mistake. Use `WithLocalDiskStorage()` with `LocalDiskStorage:RootFolder` on
the share, and `WithDistributedLock()`. Check that the share's file operations are atomic enough for compare-and-swap
(see [Local disk](06-storage-providers.md#local-disk)).

### A writer with read-only replicas

One writer with a lock, and any number of read-only replicas behind a load balancer for read-heavy clients. Writers
and readers can be separate deployments of the same image, chosen by configuration:

```csharp
if (configuration.GetValue<bool>("ReadOnly"))
    services.AddReadOnlyDtoServer<Order>().WithGoogleCloudStorage().WithCache().Build();
else
    services.AddDtoServer<Order>().WithGoogleCloudStorage().WithDistributedLock().WithCache().WithSnapshot()
        .Build("readiness");
```

Remember the replica lag, and that change feeds (`ListChanges`, `TailHistory`) on a replica see a change a few
seconds after the writer.

### Development profile

Keep the same storage code, and leave out what needs infrastructure:

```csharp
var builder = services.AddDtoServer<Order>().WithCache().WithLocalDiskStorage();
if (!environment.IsDevelopment())
    builder = builder.WithDistributedLock().WithSnapshot().WithCleanup()
        .WithBusReporting();   // your own reporter extension, see chapter 8
builder.Build("readiness");
```

A developer then runs the server with nothing but a folder.

## Stopping cleanly

On shutdown, the host stops its hosted services in reverse order of registration (or all at once with
`ServicesStopConcurrently`). For RxDatasets that means:

1. Reporters stop (registered after storage, so stopped before it). A reporter that hasn't caught up continues from
   its stored version on the next holder.
2. Storage flushes queued writes (the Google Cloud write queue).
3. The lock is released, so a standby or a new instance takes over at once instead of waiting for expiry.

Register reporters **after** the storage they consume. A server that wants every accepted write reported before the
lock moves waits for its reporters to catch up before it stops storage; the
[DataVlow server](10-datavlow-server.md) does this.

Don't pass a cancellation token with a timeout to the start of a dataset's hosted services. RxDatasets keeps the start
token for later work (initialization retries, consumers), and a token that fires later breaks them.
