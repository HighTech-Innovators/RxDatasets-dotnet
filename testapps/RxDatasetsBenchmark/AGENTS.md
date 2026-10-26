# testapps/RxDatasetsBenchmark

Console benchmark that times Demodto writes and reads against a running DTO server over gRPC.

## Files

- `Program.cs` — top-level statements. Args: `http://host:port [count]` (count default 100). Builds `count` Demodtos, then runs Put (new/unchanged/changed), PutAll (new/unchanged/changed), Get, BatchGet, List, ListChanges and Delete, 5 repeats each, resetting state with an empty or full `PutAll` before each repeat; prints min/max/avg per action.
- `RxDatasetsBenchmark.csproj` — exe; references only `../DTOs/Grpc/DemodtoV1.Grpc.csproj`.
- `RxDatasetsBenchmark.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Destructive: `PutAll` deletes every entity it does not send, so the run wipes the target dataset. Point it at `Testserver` or a throwaway bucket.
- Timings printed as "ns" are microseconds (`tpnano = TicksPerMillisecond / 1000` = 10 ticks).
- The root `Dockerfile` stages `publish-benchmark`/`final-benchmark` publish `testapps/DtoServerBenchmark`, a folder that does not exist; they target this project under its old name and fail if built.
