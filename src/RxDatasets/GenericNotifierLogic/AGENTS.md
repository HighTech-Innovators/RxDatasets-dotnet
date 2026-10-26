# src/RxDatasets/GenericNotifierLogic

The in-process, typed publish/subscribe hub that connects storage providers, lock, reader, writer and background
services of one dataset. The record types sent through it are in `../Notifications/`.

## Files
- `INotification.cs` — marker interface for notification types.
- `INotificationsHub.cs` — `INotificationsHub<TEntity>`: `Subscribe<TNotification>(Func<TNotification, Task>)` →
  `IDisposable`, `Notify<TNotification>`. `TEntity` only gives each dataset its own DI singleton.
- `NotificationsHub.cs` — `NotificationsHub<T>`, the implementation `DtoServerBuilder` registers: one unbounded channel
  and one `Task.Run` reader per subscription.
- `NotificationsHubSync.cs` — `NotificationsHubSync<T>`: a `ReplaySubject<T>(1)` per type; handlers run on the
  notifier's thread and `Notify` returns after they finish. Used by tests.

## Gotchas
- **Replay of the last notification is load-bearing.** Both hubs give a new subscriber the last notification of its
  type. Readers and writers created after the storage provider reported `Ready` initialise only because of this.
- **The two hubs behave differently.** `NotificationsHub.Notify` returns once enqueued; handlers run later, in order per
  subscription, and their exceptions are swallowed. `NotificationsHubSync` blocks each handler with `.Wait()`. Code or
  tests that assume one ordering do not hold under the other.
- `NotificationsHub` keeps a disposed subscription's channel in its bag, and `Notify` keeps writing to it.
