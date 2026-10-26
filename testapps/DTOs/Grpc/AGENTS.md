# testapps/DTOs/Grpc

Project `DemodtoV1.Grpc`: compiles the protos in `../proto/` into public C# messages plus server and client stubs.

## Files

- `DemodtoV1.Grpc.csproj` — `Protobuf Include="../proto/**/*.proto" ProtoRoot="../proto/" GrpcServices="Server,Client"` via `Grpc.Tools`; no hand-written code.
- `DemodtoV1.Grpc.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Despite the name it generates every DTO (Testdto and the shared `type` messages too); `TestdtoV1.Server`, the clients and the benchmarks all reference it.
- `ProtoRoot` fixes import paths: protos import each other as `hightechinnovators/dtos/...`, relative to `../proto/`.
- In Docker, the root `Dockerfile` (`sdk10` stage) sets `PROTOBUF_PROTOC=/usr/bin/protoc` from apt `protobuf-compiler`, replacing Grpc.Tools' bundled protoc, which fails on arm64.
