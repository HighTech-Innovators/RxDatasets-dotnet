# testapps/DTOs/proto/hightechinnovators/dtos/type

Package `hightechinnovators.dtos.type` (C# `Hightechinnovators.Dtos.Type`): DTO-independent messages imported by every `ReaderService`/`WriterService`.

## Files

- `reader_service_types.proto` — `ListChangesRequest` (`from_dataset_version`, `page_size`, `page_token`, `wait_for_changes`), `ListChangesResponse` (`to_dataset_version`, new/changed/deleted entity names, `next_page_token`), `CountChildrenRequest`/`CountChildrenResponse`.
- `writer_service_types.proto` — `WriteResponse` (`new_dataset_version`, added/updated/deleted counts), `DeleteRequest`, `PutOrDelete` (`name`, `entity_bytes`, `is_delete`).

## Gotchas

- A new `ListChangesRequest`/`ListChangesResponse` field needs hand mapping in each `*.Server/ReaderService.cs` `Map` and in the `GrpcReaderServiceImpl<T>.ListChangesRequest/Response` records (`src/RxDatasets/GrpcReaderServiceImpl.cs`).
- `PutOrDelete`: the server treats an item as a delete only when `is_delete` is true; an empty `entity_bytes` with `is_delete = false` is a put of empty bytes, whatever the field comment suggests.
