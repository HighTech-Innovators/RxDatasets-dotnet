# testapps/TestClient

Console smoke test of a running Demodto server: checks gRPC and grpc-web transport and prints each response with its latency.

## Files

- `Program.cs` — top-level statements. Args: `host:port [-web|-web2]` (no scheme; `http://` is prepended). Default is HTTP/2 cleartext gRPC (port 10080); `-web` is grpc-web forced to HTTP/1.1 (use port 18080); `-web2` is grpc-web on a default `HttpClientHandler`. Calls `ListChanges` to find the latest version, `Put`s one Demodto with a random GUID name, then pages `ListChanges` from 0 with a doubling page size. Namespace `TestClient` holds the `Logger.Log` extension and the `Http11` handler.
- `TestClient.csproj` — exe; references only `../DTOs/Grpc/DemodtoV1.Grpc.csproj`.
- `TestClient.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Every run adds a new entity under `vodproviders/TEST/vodpackages/TEST01/videofiles/cdcdcdcddeadbeef/demodtos/` to the target dataset.
