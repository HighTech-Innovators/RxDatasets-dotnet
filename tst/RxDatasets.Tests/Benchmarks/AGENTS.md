# tst/RxDatasets.Tests/Benchmarks

Opt-in memory and load-time benchmark of the index, run as xunit tests and reported through `ITestOutputHelper`.

## Files

- `IndexMemoryBenchmark.cs` — the legacy flat dictionary index against `ImmutableDatasetState` (the tree index) for two generated datasets shaped like real ids: 1M store item values and 300k ESL page images. Also defines `BenchmarkFactAttribute`.

## Gotchas

- `[BenchmarkFact]` skips unless `RXDATASETS_BENCHMARK` is set, so plain `dotnet test` and CI skip it. Run it in Release, one test at a time, for stable memory numbers (command in the class summary).
