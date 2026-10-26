# end-to-end-tests/expected-data

A MinIO single-drive data directory (`xl-single`), baked into the e2e MinIO image by `../Dockerfile` (`minio-container`). It holds the reference dataset `dtoserver-expected` serves:

- `dto-server-expected/` — the bucket. `Test/Demodto/` (root folder `Test/` + entity type name) contains `deltas/1` … `deltas/10`, each with `index.pb` and `data.pbs` (5 puts + 5 updates from `../e2e_tests.sh`), and `VersionIndex.pb`.
- `.minio.sys/` — MinIO's own state: `format.json`, `pool.bin`, bucket metadata, server config, IAM format.
- `.root_user` / `.root_password` — MinIO root credentials `ROOTUSER` / `MINIOPASSWORD`, the same test values the compose files use.

## Gotchas

- Every object is a directory holding an `xl.meta` (MinIO's on-disk format, data inlined), not a raw protobuf file. Change the data only by regenerating it with `../generate-expected-data/`.
- The dataset is in a legacy layout: `VersionIndex.pb`, no `DeltaInfo.pb`/`SnapshotInfo.pb`, no snapshots. `ObjectStorageReader` finds no `DeltaInfo.pb` and falls back to `VersionIndex.pb` (`ObsoleteV2VersionInfo`), so the e2e run also covers reading the old format. Regenerating with today's `Host` writes the current layout and drops that coverage.
- Regenerate when the reader can no longer read this layout or when an input listed under Gotchas in `../AGENTS.md` changes.
- Git tracks the `xl.meta` files only because `../.gitignore` overrides the root `*.meta` ignore.
