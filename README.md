## License

This project is licensed under the Common Public Attribution License Version 1.0 (CPAL-1.0).

You may obtain a copy of the License in the [LICENSE](./LICENSE) file.

For more details, you can also refer to the full license text at the [SPDX website](https://spdx.org/licenses/CPAL-1.0.html).

## Introduction

Reactive Datasets is a project which provides an efficient way to managing status updates of entities in protobuf format. It persisantly stores entities in a disk format (On Azure Blob, S3, or in memory). And it allows for the retrieval of entities in a reactive way, allowing clients to get a list of all changed entities since the last time they checked.

[A README for the diskformat used to store entities](src/RxDatasets/diskFormat.md)