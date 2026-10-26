# tst/RxDatasets.StorageTests/SpecTests

Concrete subclasses of the abstract spec suite in `../Common/`, one per storage backend, plus in-memory regression tests. The folder split decides what runs without Docker.

## Folders

- `UsingInMemoryTests/` — spec suites on in-memory storage, and write-path regression tests; no external services. See [`UsingInMemoryTests/AGENTS.md`](UsingInMemoryTests/AGENTS.md).
- `WithCustomTestContainersImplementation/` — spec suites on Azurite, fake-gcs-server, MinIO, LocalStack S3 and local disk, plus the MinIO gRPC end-to-end tests; excluded by name when external-provider tests are off. See [`WithCustomTestContainersImplementation/AGENTS.md`](WithCustomTestContainersImplementation/AGENTS.md).
