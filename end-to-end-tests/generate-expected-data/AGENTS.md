# end-to-end-tests/generate-expected-data

Regenerates the e2e reference dataset: runs one `final-host` server against a fresh MinIO bind-mounted to `./generated-minio-data/`, and drives it with `../e2e_tests.sh --generate-data`.

## Files

- `create_expected_data_for_new_tests.sh` — entry point; run from this folder. Deletes `./generated-minio-data`, `docker compose down -v`, build, `up -d`, `up --exit-code-from generate`, `down -v`.
- `compose.yml` — `dtoserver` (context `../..`, target `final-host`, bucket `dto-server-expected`, root folder `Test/`), `minio` (`bitnamilegacy/minio` as user 0, data at `./generated-minio-data`), `generate` (`replicas: 0`).
- `generate.Dockerfile` — context `..`; Debian with curl, jq and the linux_x86_64 grpcurl; runs `./e2e_tests.sh dtoserver:10080 --generate-data`.
- `.gitignore` — ignores `generated-minio-data/`.

## Gotchas

- The output is not moved for you: replace the contents of `../expected-data/` with `generated-minio-data/` by hand, as the script's closing message says.
- Read `../expected-data/AGENTS.md` before regenerating: the committed data is in a legacy layout that the e2e run depends on for backward-compatibility coverage.
- The script uses `#!/bin/sh` with `local` and `echo -e`; run it with bash. Its failure message prints in green.
- The x86_64 grpcurl download makes the generate image x86_64-only (unverified on arm64).
- MinIO runs as user 0, so on a Linux host the generated files are root-owned (unverified).
