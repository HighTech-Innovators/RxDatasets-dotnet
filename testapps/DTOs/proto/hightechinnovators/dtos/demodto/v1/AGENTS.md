# testapps/DTOs/proto/hightechinnovators/dtos/demodto/v1

Package `hightechinnovators.dtos.demodto.v1` (C# `Hightechinnovators.Dtos.DemodtoV1`): the Demodto entity and its gRPC services, implemented in `testapps/DTOs/DemodtoV1.Server`.

## Files

- `demodto.proto` — `Demodto`: `name` (101), `some_string`, `some_int`, `some_bool`, `some_enum` (`Quality`: `QUALITY_UNSPECIFIED`, `SD`, `HD`).
- `reader_service.proto` — `ReaderService`: `GetDemodto`, `BatchGetDemodtos`, `ListDemodtos` (paged by `page_size`/`page_token`), `ListChanges`, `CountChildren`; the last two use `type` messages.
- `writer_service.proto` — `WriterService`: `PutAll`, `PutAllChildren`, `PutMany` (client streams of `Demodto`), `Put`, `Delete`, `PutOrDeleteMany` (stream of `type.PutOrDelete`); all return `type.WriteResponse`.

## Gotchas

- `end-to-end-tests/put_request.json` is grpcurl JSON for `Demodto` using these proto field names; renaming a field breaks it.
- The comment above `Put` ("here only puts; deletes are separate") predates `PutOrDeleteMany`, which does carry deletes.
