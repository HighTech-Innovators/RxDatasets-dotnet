using Grpc.Net.Client;
using System.Diagnostics;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;

if (!args.Any())
{
    Console.Error.WriteLine("Usage: RxDatasetsBenchmark http://host:port [count=100]>");
    return;
}
var gRPCAddress = args.FirstOrDefault(arg => arg.Contains(":")) ?? "http://localhost:10080";

int inputSize = args.Skip(1).Select(arg => int.TryParse(arg, out int size) ? size: 100).FirstOrDefault(100);
int repeatCount = 5;

var sourceChannel = GrpcChannel.ForAddress(gRPCAddress);
var readerClient = new ReaderService.ReaderServiceClient(sourceChannel);
var writerClient = new WriterService.WriterServiceClient(sourceChannel);
readerClient.ListChanges(new ListChangesRequest());

List<Demodto> newDemodtos = new List<Demodto>();
List<Demodto> ChangedDemodtos = new List<Demodto>();

Console.WriteLine($"Using {inputSize} Demodtos");

for (int i = 0; i < inputSize; i++)
{
    newDemodtos.Add(new Demodto()
    {
        Name = $"vodproviders/CAPL/vodpackages/CAPL1010000000761646/vodtitle/{i}",
        SomeString = $"providerID {i}",
        SomeInt = i,
    });

    ChangedDemodtos.Add(new Demodto()
    {
        Name = $"vodproviders/CAPL/vodpackages/CAPL1010000000761646/vodtitle/{i}",
        SomeString = $"Changed providerID {i}",
        SomeInt = i * 100,
    });
}

Func<Task> PutAllEmpty = async () => await writerClient.PutAll().RequestStream.CompleteAsync();
Func<Task> PutAllDefault = async () =>
{
    var call = writerClient.PutAll();
    foreach (var vodtitle in newDemodtos)
    {
        await call.RequestStream.WriteAsync(vodtitle);
    }
    await call.RequestStream.CompleteAsync();
};
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

await Benchmark(async () => await Task.WhenAll(newDemodtos.Select(x => writerClient.PutAsync(x).ResponseAsync)), 
    "Put new", inputSize, repeatCount, PutAllEmpty);
await Benchmark(async () => newDemodtos.ForEach(x => writerClient.Put(x)), 
    "Put unchanged", inputSize, repeatCount, PutAllDefault);
await Benchmark(async () => ChangedDemodtos.ForEach(x => writerClient.Put(x)), 
    "Put Changed", inputSize, repeatCount, PutAllDefault);

await Benchmark(async () => {
    var call = writerClient.PutAll();
    foreach (var subtitlefile in newDemodtos)
    {
        await call.RequestStream.WriteAsync(subtitlefile);
    }
    await call.RequestStream.CompleteAsync();
}, "Put All new", inputSize, repeatCount, PutAllEmpty);
await Benchmark(async () => {
    var call = writerClient.PutAll();
    foreach (var subtitlefile in newDemodtos)
    {
        await call.RequestStream.WriteAsync(subtitlefile);
    }
    await call.RequestStream.CompleteAsync();
}, "Put All unchanged", inputSize, repeatCount);
await Benchmark(async () => {
    var call = writerClient.PutAll();
    foreach (var subtitlefile in ChangedDemodtos)
    {
        await call.RequestStream.WriteAsync(subtitlefile);
    }
    await call.RequestStream.CompleteAsync();
}, "Put All Changed", inputSize, repeatCount, PutAllDefault);

var batchGet = new BatchGetDemodtosRequest();
List<string> names = newDemodtos.Select(x => x.Name).ToList();
batchGet.Names.AddRange(names);

await Benchmark(async () => names.ForEach(x => readerClient.GetDemodto(new GetDemodtoRequest() { Name = x })), 
    "Get", inputSize, repeatCount);
await Benchmark(async () => await readerClient.BatchGetDemodtosAsync(batchGet), 
    "Batch Get", inputSize, repeatCount);

await Benchmark(async () => readerClient.ListDemodtos(new ListDemodtosRequest() { PageSize = inputSize, Parent = "parent" }), 
    "List", inputSize, repeatCount);
await Benchmark(async () => readerClient.ListChanges(new ListChangesRequest() { FromDatasetVersion = 0 }), 
    "List changes", inputSize, repeatCount);

await Benchmark(async () => names.ForEach(x => writerClient.Delete(new DeleteRequest() { Name = x })), 
    "delete", inputSize, repeatCount, PutAllDefault);
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously

static async Task Benchmark(Func<Task> taskToBenchmark, string actionName, int itemCount, int repeat = 1, Func<Task>? initializeTask = null)
{
    //Console.WriteLine($"Benchmarking {actionName}");

    long tpnano = TimeSpan.TicksPerMillisecond / 1000;

    List<long> results = new();

    for(int i = 0; i < repeat; i++)
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