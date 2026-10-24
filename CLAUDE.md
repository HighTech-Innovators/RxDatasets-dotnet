# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RxDatasets is a .NET library for managing reactive, delta-based storage of protobuf entities. It stores entities using a delta/snapshot pattern on various cloud backends (Azure Blob, S3, Google Cloud, Minio, local disk) and provides reactive change notifications via System.Reactive.

## Build Commands

```bash
dotnet restore
dotnet build
dotnet test                                    # run all tests
dotnet test --filter "FullyQualifiedName~ClassName.MethodName"  # run a single test
dotnet test tst/RxDatasets.Tests               # run tests in a specific project
```

CI pipeline stages: restore → build → test → publish → push → sonarqube-check

## Architecture

### Core Abstraction Layer (`src/RxDatasets.Abstractions`)
Defines the key interfaces:
- **`IDatasetReader<T>`** — query operations: `Get`, `BatchGet`, `List`, `GetChangedNames`, `WaitChangedNames` (reactive), `GetHistory`, `StreamHistory`, `TailHistory`, `CountChildren`
- **`IDatasetWriter<T>`** — mutations: `Put`, `PutMany`, `PutAll`, `Delete`, `DeleteMany`, `PutOrDeleteMany`. Returns `WriteResponse` with version and change counts
- **`IDiskStorage<TEntity>`** — file-based storage abstraction
- **`IStorageProvider`** — backend-agnostic blob read/write

### Storage Format (Delta/Snapshot Pattern)
Entities are serialized as protobuf. Each write creates a **delta** (`deltas/{version}/index.pb` + `data.pbs`). Periodic **snapshots** (`snapshot/{version}/index.pb` + `data.pbs`) aggregate full state. Metadata files `DeltaInfo.pb` and `SnapshotInfo.pb` track available versions. See `src/RxDatasets/diskFormat.md` for full format specification.

### Key Implementation Components (`src/RxDatasets`)
- **`DtoServerBuilder<TEntity>`** — fluent API for configuring dataset servers with storage, caching, snapshots, cleanup
- **`ObjectStorageProvider<TEntity>`** — main storage implementation handling delta/snapshot read/write
- **`CacheStorageProvider<TEntity>`** — LRU caching layer
- **`DistributedLockService`** — coordinates multi-instance write access
- **`SnapshotCreationPlugin`** — background periodic snapshot creation
- **`StorageCleaningService`** — cleanup of old deltas/snapshots based on retention policy

### Storage Providers (`src/RxDatasets.StorageProvider.*`)
Each provider implements `IStorageProvider` for a specific backend: LocalDisk, S3, AzBlob, GoogleCloud, Minio.

### Supporting Libraries
- **`RxDatasets.InMemoryDataset`** — in-memory dataset (useful for testing)
- **`RxDatasets.DatasetWatcher`** — monitors dataset changes
- **`RxDatasets.Reporting`** — dataset update reporting
- **`RxDatasets.Aliasing`** — alias-based entity reader wrapper (see `src/RxDatasets.Aliasing/README.md`)

## Project Conventions

- **Target framework**: .NET 10.0
- **Centralized package versions**: `Directory.Packages.props` — do not pin versions in individual csproj files
- **Implicit usings** and **nullable reference types** are enabled project-wide
- **Test frameworks**: xunit, FluentAssertions, NSubstitute, Moq, Testcontainers (for integration tests)
- **Code style**: `.editorconfig` enforced — 4-space indentation, opening braces on new lines, max 130 char lines, accessibility modifiers required, no `var` without apparent type
- **CI**: GitLab CI (`.gitlab-ci.yml`), SonarQube for quality gates
- **License**: CPAL-1.0
