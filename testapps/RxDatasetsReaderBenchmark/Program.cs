using System.Diagnostics;
using Grpc.Net.Client;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;

if (!args.Any())
{
    Console.Error.WriteLine("Usage: RxDatasetsBenchmark http://host:port [count=100]>");
    return;
}
var gRPCAddress = args.FirstOrDefault(arg => arg.Contains(":")) ?? "http://localhost:10080";

int inputSize = args.Skip(1).Select(arg => int.TryParse(arg, out int size) ? size : 1000).FirstOrDefault(1000);
int repeatCount = 5;

var sourceChannel = GrpcChannel.ForAddress(gRPCAddress);
var readerClient = new ReaderService.ReaderServiceClient(sourceChannel);

var responseList = readerClient.ListChanges(new ListChangesRequest());


Console.WriteLine($"Using {inputSize} Demodtos");


var batchGet = new BatchGetDemodtosRequest();
var allVodTitles = readerClient.ListDemodtos(new ListDemodtosRequest()).Demodtos;


List<string> names = allVodTitles.Select(x => x.Name).ToList();
batchGet.Names.AddRange(names);
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
await Benchmark(async () => names.ForEach(x => readerClient.GetDemodto(new GetDemodtoRequest() { Name = x })),
    "Get", inputSize, repeatCount);
await Benchmark(async () => await readerClient.BatchGetDemodtosAsync(batchGet),
    "Batch Get", inputSize, repeatCount);

await Benchmark(async () => readerClient.ListDemodtos(new ListDemodtosRequest() { PageSize = inputSize, Parent = "parent" }),
    "List", inputSize, repeatCount);
await Benchmark(async () => readerClient.ListChanges(new ListChangesRequest() { FromDatasetVersion = 0 }),
    "List changes", inputSize, repeatCount);
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
static async Task Benchmark(Func<Task> taskToBenchmark, string actionName, int itemCount, int repeat = 1, Func<Task>? initializeTask = null)
{
    //Console.WriteLine($"Benchmarking {actionName}");

    long tpnano = TimeSpan.TicksPerMillisecond / 1000;

    List<long> results = new();

    for (int i = 0; i < repeat; i++)
    {
        if (initializeTask != null)
            await initializeTask.Invoke();

        Stopwatch stopwatch = Stopwatch.StartNew();
        await taskToBenchmark.Invoke();
        results.Add(stopwatch.Elapsed.Ticks / tpnano);
    }

    results.Sort();
    Console.WriteLine($"{actionName,20} took {results.Min(),5} ns - {results.Max(),5} ns (avg {(long)results.Average(),5}) ns; {(long)(results.Average() / itemCount),5} ns per item");
}
