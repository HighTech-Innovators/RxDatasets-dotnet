###### Sonar Statistics:
- [![Quality Gate Status](https://sonar.developer.htiprojects.nl/api/project_badges/measure?project=oss_reactive-datasets_8d09a016-e5e6-4c6a-a047-59be74e6adfc&metric=alert_status&token=sqb_9fbd6ed9645a4778a27db7fbb01df9072047af09)](https://sonar.developer.htiprojects.nl/dashboard?id=oss_reactive-datasets_8d09a016-e5e6-4c6a-a047-59be74e6adfc)

## License

  

This project is licensed under the Common Public Attribution License Version 1.0 (CPAL-1.0).

  

You may obtain a copy of the License in the [LICENSE](./LICENSE) file.

  

For more details, you can also refer to the full license text at the [SPDX website](https://spdx.org/licenses/CPAL-1.0.html).

  

## Introduction

  

Reactive Datasets is a project which provides an efficient way to managing status updates of entities in protobuf format. It persistently stores entities in a disk format (On Azure Blob, S3, or in memory). And it allows for the retrieval of entities in a reactive way, allowing clients to get a list of all changed entities since the last time they checked.

  

[A README for the diskformat used to store entities](src/RxDatasets/diskFormat.md)

  

## Configurations

### Cleanup Config Options

- MinimumAgeToLiveInMinutes - must be at least 120 minutes (default)

- MinimumSnapshotsToKeep - must be at least 2 snapshots kept (default)

- KeepAllDeltas = true (default); // When disabled, all deltas leading to the outdated snapshot would be deleted,
e.g. lets say we have deltas [1,2,3,4,5,6,7,8,9,10] and snapshots [1,4,7, 10] (assuming a snapshot interval of 3),
when snapshot 1,4 are deleted, deltas 1,2,3,4,5,6 would also be deleted since the earliest to keep snapshot would 7

- IsEnabled = true (default);

