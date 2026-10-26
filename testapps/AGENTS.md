# testapps

Runnable apps that host RxDatasets behind gRPC or drive it: DTO servers, the demo DTO contract, a client and benchmarks. All are in `RxDatasets.sln` and non-packable; the root `Dockerfile` builds images from `Host` (`final-host`) and `Testserver` (`final-testcontainer`).

## Folders

- `DTOs/` — the Demodto/Testdto proto contract, its generated gRPC assembly and the per-DTO gRPC service glue. See [DTOs/AGENTS.md](DTOs/AGENTS.md).
- `Host/` — read-write Demodto server on S3/MinIO; the image the e2e tests and root `compose.yml` run. See [Host/AGENTS.md](Host/AGENTS.md).
- `ReadOnlyHost/` — read-only Demodto server on S3. See [ReadOnlyHost/AGENTS.md](ReadOnlyHost/AGENTS.md).
- `Testserver/` — read-write Demodto server on in-memory storage; the `Program` behind `tst/RxDatasets.StorageTests`. See [Testserver/AGENTS.md](Testserver/AGENTS.md).
- `TestClient/` — console smoke-test client (gRPC or grpc-web). See [TestClient/AGENTS.md](TestClient/AGENTS.md).
- `RxDatasetsBenchmark/` — write + read latency benchmark against a running server. See [RxDatasetsBenchmark/AGENTS.md](RxDatasetsBenchmark/AGENTS.md).
- `RxDatasetsReaderBenchmark/` — read-only latency benchmark against a running server. See [RxDatasetsReaderBenchmark/AGENTS.md](RxDatasetsReaderBenchmark/AGENTS.md).
- `RxDatasetsGcsBenchmark/` — in-process write-throughput benchmark on Google Cloud Storage, no gRPC. See [RxDatasetsGcsBenchmark/AGENTS.md](RxDatasetsGcsBenchmark/AGENTS.md).

## Gotchas

- Port plan: `Host` and `Testserver` both bind 10080 (gRPC, HTTP/2 cleartext) and 18080 (HTTP/1.1: grpc-web, health); `ReadOnlyHost` uses 10081/18081 so it can run beside one of them. All three skip these listeners when the environment is `Testing`.
- Each `*.v3.ncrunchproject` is the same NCrunch settings file (hides `UnnecessaryFileIncludeWarning`).
