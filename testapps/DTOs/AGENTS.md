# testapps/DTOs

Demo entity contract and its server glue. Build chain: `proto/` → `Grpc/` (project `DemodtoV1.Grpc`, generates C# for every proto) → `DemodtoV1.Server/`, `TestdtoV1.Server/` → the hosts in `testapps/`.

## Folders

- `proto/` — `.proto` sources for Demodto, Testdto and shared request/response types. See [proto/AGENTS.md](proto/AGENTS.md).
- `Grpc/` — `DemodtoV1.Grpc` library compiling all protos to server and client stubs. See [Grpc/AGENTS.md](Grpc/AGENTS.md).
- `DemodtoV1.Server/` — gRPC Reader/Writer services for `Demodto` plus DI helpers. See [DemodtoV1.Server/AGENTS.md](DemodtoV1.Server/AGENTS.md).
- `TestdtoV1.Server/` — gRPC Reader/Writer services for `Testdto` plus DI helpers. See [TestdtoV1.Server/AGENTS.md](TestdtoV1.Server/AGENTS.md).

## Gotchas

- `Demodto2V1.Server/` holds only stale, untracked `bin/obj` output of a removed project: no csproj, not in the solution.
- Adding a DTO touches four places: a `proto/hightechinnovators/dtos/<dto>/v1/` folder (picked up by the `Grpc` glob), a `<Dto>V1.Server` project, the host's `ConfigureServices` (both the server's `DI.ConfigureServices` and `AddDtoServer<T>()…Build()`), and the host's `ConfigureApp`.
