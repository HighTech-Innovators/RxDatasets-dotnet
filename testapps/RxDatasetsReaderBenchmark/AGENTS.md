# testapps/RxDatasetsReaderBenchmark

Console benchmark that times Demodto reads against a running DTO server over gRPC, leaving the data untouched.

## Files

- `Program.cs` — top-level statements. Args: `http://host:port [count]` (count default 1000). Fetches existing names with one unpaged `ListDemodtos`, then times Get (one call per name), BatchGet, List (`page_size = count`) and ListChanges from version 0, 5 repeats each. `count` only sets the List page size and the per-item divisor.
- `RxDatasetsReaderBenchmark.csproj` — exe with `InvariantGlobalization`; references only `../DTOs/Grpc/DemodtoV1.Grpc.csproj`.
- `RxDatasetsReaderBenchmark.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Seed the dataset first (e.g. with `RxDatasetsBenchmark` or `e2e_tests.sh`); on an empty dataset Get/BatchGet time nothing.
- Timings printed as "ns" are microseconds, as in `RxDatasetsBenchmark`.
- The root `Dockerfile` stages `publish-reader-benchmark`/`final-reader-benchmark` publish `testapps/DtoServerReaderBenchmark`, which does not exist; they target this project under its old name.
