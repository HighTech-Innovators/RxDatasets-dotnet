# end-to-end-tests

Docker Compose regression test of the `final-host` image (`testapps/Host`). One MinIO image is pre-seeded with `expected-data/`. `dtoserver-actual` writes to the empty bucket `dto-server-generated`, `dtoserver-expected` serves the seeded bucket `dto-server-expected`, both with root folder `Test/`. The `tests` container runs `e2e_tests.sh`: 5 Demodto `Put`s and 5 updates against actual, then compares `GetDemodto` per name and `ListChanges` from version 3 between the two servers. CI job `e2e test` runs `VERSION=... ./run.sh` from this folder.

## Files

- `run.sh` — entry point. Compose project name `$CI_JOB_ID` (default `1`); `VERSION` defaults to `1.1.1`. Builds, runs `up -d` (up to 30 tries, 5 s apart), then `up --exit-code-from tests`, prints logs, `down -v --rmi local`, exits with the tests' code.
- `compose.yml` — `tests` (`replicas: 0`, so `up -d` skips it), `dtoserver-actual`/`dtoserver-expected` (context `..`, target `final-host`, `S3__*` + `Storage__CustomRootFolder=Test/` env), `minio` (target `minio-container`).
- `Dockerfile` — `minio-container`: `bitnamilegacy/minio` with `expected-data/` copied to `/bitnami/minio/data/` (owner 1001); `end-to-end-test`: `bash` image plus grpcurl, jq, curl, running `e2e_tests.sh dtoserver-actual:10080 dtoserver-expected:10080`.
- `e2e_tests.sh` — args `<server1> [<server2> | --generate-data]`. Waits up to 120 s for `grpcurl list` and `:18080/healthz` = `Healthy`, writes, checks each `WriteResponse` count, compares JSON with sorted keys and arrays. `--generate-data` only writes (used by `generate-expected-data/`).
- `put_request.json` — base Demodto in grpcurl JSON; `name` and `some_string` are overwritten per call.
- `.dockerignore` — context holds only `expected-data/`, `e2e_tests.sh`, `put_request.json`.
- `.gitattributes` — LF endings for `*.sh` and Dockerfiles; `*.meta` binary; rules for a `TestData/` folder that does not exist.
- `.gitignore` — re-includes everything here, including `*.meta`, which the root `.gitignore` ignores.

## Folders

- `expected-data/` — MinIO data directory with the reference dataset. See [expected-data/AGENTS.md](expected-data/AGENTS.md).
- `generate-expected-data/` — compose setup that regenerates that dataset. See [generate-expected-data/AGENTS.md](generate-expected-data/AGENTS.md).

## Gotchas

- The assertions are coupled to `expected-data/`: the 5 names and strings in `e2e_tests.sh`, `put_request.json`, the `Demodto` schema and the version numbers (`ListChanges` from 3). Change any of them and regenerate the data.
- `run.sh` starts with `#!/bin/sh` but uses `[[ ]]`, `local` and `echo -e`; run it with bash. CI runs `./run.sh` in `${DOCKER_BUILD_IMAGE}`, which works only where `sh` accepts these (unverified).
- `e2e_tests.sh`'s header says to run it via `run_me.sh`; the file is `run.sh`.
- `e2e_tests.sh` installs missing tools with `sudo` and downloads the linux_arm64 grpcurl; the test image already has grpcurl, so this only fires on a local machine, where x86_64 gets the wrong binary.
- `check_server` probes `/healthz` on port 18080 whatever gRPC port it is given.
- Inert build args: compose passes `DOCKERHOST` and `VERSION`, but the root `Dockerfile` declares `DOCKER_HOST` (test stage only) and uses `VERSION` only in release stages; `final-host` is built from the `build` stage at `0.0.0-local`.
- Images are unpinned: `debian:latest`, `bash`, `fullstorydev/grpcurl`, `ghcr.io/jqlang/jq`, `ghcr.io/tarampampam/curl`, `bitnamilegacy/minio`.
