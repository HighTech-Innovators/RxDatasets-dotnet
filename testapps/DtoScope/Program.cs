using Google.Protobuf;
using RxDatasets.Models;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace DtoScope;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            // Handle help first
            if (args.Length == 0 || args[0] == "-h" || args[0] == "--help")
            {
                PrintUsage();
                return args.Length == 0 ? 1 : 0;
            }

            var filePath = args[0];
            var file = new FileInfo(filePath);

            if (!file.Exists)
            {
                Console.Error.WriteLine($"Error: File not found: {file.FullName}");
                return 1;
            }

            var options = ParseOptions(args.Skip(1).ToArray());

            var bytes = await File.ReadAllBytesAsync(file.FullName);

            ProtoType detectedType = DetermineType(file, options, bytes);

            if (options.Verbose)
                Console.WriteLine($"[File: {file.Name}] [Type: {detectedType}] [Size: {bytes.Length} bytes]\n");

            string yaml = DecodeToYaml(bytes, detectedType);
            Console.WriteLine(yaml);

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine(@"
DtoScope - RxDatasets DTO inspector and YAML converter

Usage: dtoscope <file> [options]

Options:
  -s, --snapshot      Treat as SnapshotIndex
  -d, --delta         Treat as DeltaIndex
  -S, --snapshotinfo  Treat as SnapshotInfo
  -D, --deltainfo     Treat as DeltaInfo
  -P, --processedinfo Treat as ProcessedVersionsInfo
  -v, --verbose       Show detailed output
  -h, --help          Show this help message

Examples:
  dtoscope snapshot/0/index.pb
  dtoscope deltas/1/index.pb -d
  dtoscope snapshot/0/info.pb -S
");
    }

    private class Options
    {
        public bool? Snapshot { get; set; }
        public bool? Delta { get; set; }
        public bool? SnapshotInfo { get; set; }
        public bool? DeltaInfo { get; set; }
        public bool? ProcessedInfo { get; set; }
        public bool Verbose { get; set; }
    }

    private static Options ParseOptions(string[] args)
    {
        var options = new Options();

        foreach (var arg in args)
        {
            switch (arg.ToLowerInvariant())
            {
                case "-s":
                case "--snapshot":
                    options.Snapshot = true;
                    break;
                case "-d":
                case "--delta":
                    options.Delta = true;
                    break;
                case "-S":
                case "--snapshotinfo":
                    options.SnapshotInfo = true;
                    break;
                case "-D":
                case "--deltainfo":
                    options.DeltaInfo = true;
                    break;
                case "-P":
                case "--processedinfo":
                    options.ProcessedInfo = true;
                    break;
                case "-v":
                case "--verbose":
                    options.Verbose = true;
                    break;
                case "-h":
                case "--help":
                    PrintUsage();
                    Environment.Exit(0);
                    break;
                default:
                    Console.Error.WriteLine($"Unknown option: {arg}");
                    PrintUsage();
                    Environment.Exit(1);
                    break;
            }
        }

        return options;
    }

    private static ProtoType DetermineType(FileInfo file, Options options, byte[] bytes)
    {
        // Check explicit flags first (highest priority)
        if (options.Snapshot == true) return ProtoType.Snapshot;
        if (options.Delta == true) return ProtoType.Delta;
        if (options.SnapshotInfo == true) return ProtoType.SnapshotInfo;
        if (options.DeltaInfo == true) return ProtoType.DeltaInfo;
        if (options.ProcessedInfo == true) return ProtoType.ProcessedInfo;

        // Auto-detect from filename pattern
        var fileName = file.Name;
        if (fileName.Contains("snapshot", StringComparison.OrdinalIgnoreCase))
        {
            if (fileName.EndsWith("info.pb", StringComparison.OrdinalIgnoreCase))
                return ProtoType.SnapshotInfo;
            return ProtoType.Snapshot;
        }

        if (fileName.Contains("delta", StringComparison.OrdinalIgnoreCase))
        {
            if (fileName.EndsWith("info.pb", StringComparison.OrdinalIgnoreCase))
                return ProtoType.DeltaInfo;
            return ProtoType.Delta;
        }

        if (fileName.Contains("processed", StringComparison.OrdinalIgnoreCase) ||
            fileName.Contains("versions", StringComparison.OrdinalIgnoreCase))
            return ProtoType.ProcessedInfo;

        // Last resort: try parsing each type and see which works
        return AutoDetectByParsing(bytes);
    }

    private static ProtoType AutoDetectByParsing(byte[] bytes)
    {
        // Try SnapshotIndex first (most common)
        try
        {
            _ = SnapshotIndex.Parser.ParseFrom(bytes);
            return ProtoType.Snapshot;
        }
        catch { }

        // Try DeltaIndex
        try
        {
            _ = DeltaIndex.Parser.ParseFrom(bytes);
            return ProtoType.Delta;
        }
        catch { }

        // Try SnapshotInfo
        try
        {
            _ = SnapshotInfo.Parser.ParseFrom(bytes);
            return ProtoType.SnapshotInfo;
        }
        catch { }

        // Try DeltaInfo
        try
        {
            _ = DeltaInfo.Parser.ParseFrom(bytes);
            return ProtoType.DeltaInfo;
        }
        catch { }

        // Try ProcessedVersionsInfo
        try
        {
            _ = ProcessedVersionsInfo.Parser.ParseFrom(bytes);
            return ProtoType.ProcessedInfo;
        }
        catch { }

        throw new InvalidOperationException(
            "Could not auto-detect proto type. Use -s, -d, -S, -D, or -P flags to specify type.");
    }

    private static string DecodeToYaml(byte[] bytes, ProtoType type)
    {
        IMessage message = type switch
        {
            ProtoType.Snapshot => SnapshotIndex.Parser.ParseFrom(bytes),
            ProtoType.Delta => DeltaIndex.Parser.ParseFrom(bytes),
            ProtoType.SnapshotInfo => SnapshotInfo.Parser.ParseFrom(bytes),
            ProtoType.DeltaInfo => DeltaInfo.Parser.ParseFrom(bytes),
            ProtoType.ProcessedInfo => ProcessedVersionsInfo.Parser.ParseFrom(bytes),
            _ => throw new ArgumentException($"Unknown type: {type}")
        };

        var serializer = new SerializerBuilder()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build();

        return serializer.Serialize(message);
    }
}

public enum ProtoType
{
    Snapshot,
    Delta,
    SnapshotInfo,
    DeltaInfo,
    ProcessedInfo
}
