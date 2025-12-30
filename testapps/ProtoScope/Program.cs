using Google.Protobuf;
using RxDatasets.Models;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;
using System.Collections;

namespace ProtoScope;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            // Handle version first
            if (args.Length > 0 && (args[0] == "--version" || args[0] == "-v"))
            {
                PrintVersion();
                return 0;
            }

            // Handle help
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

            string yaml = DecodeToYaml(bytes, detectedType, options.ShowAll);
            Console.WriteLine(yaml);

            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
    }

    private static void PrintVersion()
    {
        var assembly = typeof(Program).Assembly;
        var version = assembly.GetName().Version?.ToString() ?? "unknown";
        var informationalVersionAttr = assembly.GetCustomAttributes(false)
            .OfType<System.Reflection.AssemblyInformationalVersionAttribute>()
            .FirstOrDefault();
        var informationalVersion = informationalVersionAttr?.InformationalVersion ?? version;
        
        Console.WriteLine($"DtoScope version {informationalVersion}");
    }

    private static void PrintUsage()
    {
        Console.WriteLine(@"
ProtoScope - Protobuf file inspector and YAML converter

Usage: protoscope <file> [options]

Options:
  -s, --snapshot      Treat as SnapshotIndex
  -d, --delta         Treat as DeltaIndex
  -S, --snapshotinfo  Treat as SnapshotInfo
  -D, --deltainfo     Treat as DeltaInfo
  -P, --processedinfo Treat as ProcessedVersionsInfo
  --verbose           Show detailed output
  --show-all          Show empty arrays and default values (omitted by default)
  --version           Show version and exit
  -h, --help          Show this help message

Examples:
  protoscope snapshot/0/index.pb
  protoscope deltas/1/index.pb -d
  protoscope snapshot/0/info.pb -S
  protoscope --version
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
        public bool ShowAll { get; set; }  // Show empty arrays and default values
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
                case "--verbose":
                    options.Verbose = true;
                    break;
                case "--show-all":
                    options.ShowAll = true;
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
        // Try parsing with each type and score based on data population
        var candidates = new List<(ProtoType type, IMessage message, int score)>();

        // Try SnapshotIndex
        try
        {
            var msg = SnapshotIndex.Parser.ParseFrom(bytes);
            int score = ScoreSnapshotIndex(msg);
            if (score > 0)
                candidates.Add((ProtoType.Snapshot, msg, score));
        }
        catch { }

        // Try DeltaIndex
        try
        {
            var msg = DeltaIndex.Parser.ParseFrom(bytes);
            int score = ScoreDeltaIndex(msg);
            if (score > 0)
                candidates.Add((ProtoType.Delta, msg, score));
        }
        catch { }

        // Try SnapshotInfo
        try
        {
            var msg = SnapshotInfo.Parser.ParseFrom(bytes);
            int score = ScoreSnapshotInfo(msg);
            if (score > 0)
                candidates.Add((ProtoType.SnapshotInfo, msg, score));
        }
        catch { }

        // Try DeltaInfo
        try
        {
            var msg = DeltaInfo.Parser.ParseFrom(bytes);
            int score = ScoreDeltaInfo(msg);
            if (score > 0)
                candidates.Add((ProtoType.DeltaInfo, msg, score));
        }
        catch { }

        // Try ProcessedVersionsInfo
        try
        {
            var msg = ProcessedVersionsInfo.Parser.ParseFrom(bytes);
            int score = ScoreProcessedVersionsInfo(msg);
            if (score > 0)
                candidates.Add((ProtoType.ProcessedInfo, msg, score));
        }
        catch { }

        if (candidates.Count == 0)
            throw new InvalidOperationException(
                "Could not auto-detect proto type. Use -s, -d, -S, -D, or -P flags to specify type.");

        // Return the candidate with the highest score
        var best = candidates.OrderByDescending(c => c.score).First();
        return best.type;
    }

    private static int ScoreSnapshotIndex(SnapshotIndex msg)
    {
        int score = 0;
        if (msg.SnapshotVersion != 0) score += 10;
        if (msg.Entities.Count > 0) score += 20;
        if (msg.Root != null && msg.Root.Children.Count > 0) score += 15;
        
        // Heavily penalize unknown fields - indicates wrong type
        int unknownSize = GetUnknownFieldsSize(msg);
        if (unknownSize > 100)
            score -= unknownSize / 10;  // Strong negative signal
            
        return Math.Max(0, score);
    }

    private static int ScoreDeltaIndex(DeltaIndex msg)
    {
        int score = 0;
        if (msg.DeltaVersion != 0) score += 10;
        if (msg.ChangedEntities.Count > 0) score += 20;
        if (msg.NewEntities.Count > 0) score += 20;
        if (msg.DeletedNames.Count > 0) score += 15;
        if (msg.DeletedEntities.Count > 0) score += 15;
        if (msg.Root != null && msg.Root.Children.Count > 0) score += 15;
        
        // Heavily penalize unknown fields - indicates wrong type
        int unknownSize = GetUnknownFieldsSize(msg);
        if (unknownSize > 100)
            score -= unknownSize / 10;
            
        return Math.Max(0, score);
    }

    private static int ScoreSnapshotInfo(SnapshotInfo msg)
    {
        int score = msg.SnapshotVersions.Count > 0 ? 20 : 0;
        
        // Heavily penalize unknown fields
        int unknownSize = GetUnknownFieldsSize(msg);
        if (unknownSize > 100)
            score -= unknownSize / 10;
            
        return Math.Max(0, score);
    }

    private static int ScoreDeltaInfo(DeltaInfo msg)
    {
        int score = 0;
        if (msg.EarliestDeltaVersion != 0) score += 10;
        if (msg.LatestDeltaVersion != 0) score += 10;
        
        // Heavily penalize unknown fields
        int unknownSize = GetUnknownFieldsSize(msg);
        if (unknownSize > 100)
            score -= unknownSize / 10;
            
        return Math.Max(0, score);
    }

    private static int ScoreProcessedVersionsInfo(ProcessedVersionsInfo msg)
    {
        int score = msg.ProcessedVersions.Count > 0 ? 20 : 0;
        
        // Heavily penalize unknown fields
        int unknownSize = GetUnknownFieldsSize(msg);
        if (unknownSize > 100)
            score -= unknownSize / 10;
            
        return Math.Max(0, score);
    }

    private static int GetUnknownFieldsSize(IMessage msg)
    {
        // Access the message's UnknownFields via reflection
        var unknownFieldsField = msg.GetType().GetProperty("UnknownFields", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        if (unknownFieldsField?.GetValue(msg) is UnknownFieldSet unknownFieldSet)
        {
            // Use CalculateSize method if available
            var sizeMethod = unknownFieldSet.GetType().GetMethod("CalculateSize",
                System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            if (sizeMethod != null)
            {
                var result = sizeMethod.Invoke(unknownFieldSet, null);
                return result != null ? (int)result : 0;
            }
        }
        
        return 0;
    }

    private static string DecodeToYaml(byte[] bytes, ProtoType type, bool showAll = false)
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

        // Use JsonFormatter to handle default values and empty collections correctly
        // This ensures we only output what's actually present in the proto file (unless showAll is true)
        var settings = new JsonFormatter.Settings(showAll);
        var formatter = new JsonFormatter(settings);
        string json = formatter.Format(message);

        // Convert JSON to YAML via an intermediate object
        var deserializer = new DeserializerBuilder().Build();
        var yamlObject = deserializer.Deserialize(new StringReader(json));

        var serializer = new SerializerBuilder()
            .DisableAliases()
            .Build();
        
        return serializer.Serialize(yamlObject);
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
