# testapps/DTOs/proto

Proto root (`ProtoRoot`) for `../Grpc`. Per-DTO contracts live in `hightechinnovators/dtos/<dto>/v1/`, shared request/response messages in `hightechinnovators/dtos/type/`.

## Folders

- `hightechinnovators/` — the only path into the tree. See [hightechinnovators/AGENTS.md](hightechinnovators/AGENTS.md).

## Conventions (followed by every file)

- `package` = folder path with dots (`hightechinnovators.dtos.demodto.v1`); `csharp_namespace` = `Hightechinnovators.Dtos.<Dto>V1` or `Hightechinnovators.Dtos.Type`; `java_package` mirrors the package under `com.`.
- A DTO folder holds `<dto>.proto` (the entity), `reader_service.proto` (`ReaderService`) and `writer_service.proto` (`WriterService`). Service names repeat across DTOs; the package tells them apart.
- The entity's resource key is `string name = 101`; payload fields start at 1.

## Gotchas

- `csharp_namespace` is load-bearing: the `*.Server` projects, hosts, client, benchmarks and `tst/RxDatasets.StorageTests` import `Hightechinnovators.Dtos.DemodtoV1` / `.Type`.
- The proto package is part of the wire service name: `end-to-end-tests/e2e_tests.sh` calls `hightechinnovators.dtos.demodto.v1.ReaderService/GetDemodto` and `.WriterService/Put` through grpcurl reflection.
- Entities are stored as protobuf bytes, so `Demodto` field numbers/types are frozen into `end-to-end-tests/expected-data`; a schema change there means regenerating that data.
