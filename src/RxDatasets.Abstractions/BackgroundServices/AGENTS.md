# src/RxDatasets.Abstractions/BackgroundServices

The state enum of core `DistributedLockService<T>`.

## Files

- `LockServiceState.cs` — `Idle`, `Acquiring` (request file written), `Active` (renewing), `Yielded` (released,
  waiting for the handover timeout); exposed as `DistributedLockService<T>.State`.

## Gotchas

- Its namespace is `RxDatasets.BackgroundServices`, the core's background-services namespace, so
  `using RxDatasets.BackgroundServices;` brings in this enum together with core types such as `IClock`.
