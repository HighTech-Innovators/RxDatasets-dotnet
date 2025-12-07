using System;
using System.IO;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

// The namespace depends on the proto file. 
// Since there is no package defined, it might be just the class name in global namespace, 
// or derived from filename. 
// However, in the main project it seems to be used as if it's in RxDatasets.Models or similar.
// Let's try without namespace first, or check if we can find it.
// Actually, if I link the file, the behavior is standard protoc behavior.
// Without 'package', it uses the filename as namespace? No, that's Java.
// C# default is the file name in PascalCase?
// Let's try to assume it's in the global namespace or 'DatasetEntities'.

class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: TimestampFixer <directory>");
            return;
        }

        string rootDir = args[0];
        if (!Directory.Exists(rootDir))
        {
            Console.WriteLine($"Directory not found: {rootDir}");
            return;
        }

        ProcessDirectory(rootDir);
    }

    static void ProcessDirectory(string dir)
    {
        // Check if this is a 'deltas' directory
        if (Path.GetFileName(dir).Equals("deltas", StringComparison.OrdinalIgnoreCase))
        {
            ProcessDeltasDirectory(dir);
        }

        // Recurse
        try
        {
            foreach (var subDir in Directory.GetDirectories(dir))
            {
                ProcessDirectory(subDir);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error accessing {dir}: {ex.Message}");
        }
    }

    static void ProcessDeltasDirectory(string deltasDir)
    {
        Console.WriteLine($"Processing deltas in: {deltasDir}");
        try
        {
            foreach (var versionDir in Directory.GetDirectories(deltasDir))
            {
                var indexFile = Path.Combine(versionDir, "index.pb");
                if (File.Exists(indexFile))
                {
                    ProcessIndexFile(indexFile);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error accessing {deltasDir}: {ex.Message}");
        }
    }

    static void ProcessIndexFile(string filePath)
    {
        try
        {
            var bytes = File.ReadAllBytes(filePath);
            // Try to parse. If DeltaIndex is not found, we will get a build error and I will fix it.
            var deltaIndex = DeltaIndex.Parser.ParseFrom(bytes);
            bool changed = false;

            if (deltaIndex.Timestamp == null || (deltaIndex.Timestamp.Seconds == 0 && deltaIndex.Timestamp.Nanos == 0))
            {
                var fileInfo = new FileInfo(filePath);
                var lastWriteTime = fileInfo.LastWriteTimeUtc;
                deltaIndex.Timestamp = Timestamp.FromDateTime(lastWriteTime);
                changed = true;
                Console.WriteLine($"Updated timestamp for {filePath} to {lastWriteTime}");
            }

            if (changed)
            {
                using (var output = File.OpenWrite(filePath))
                {
                    output.SetLength(0);
                    deltaIndex.WriteTo(output);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing {filePath}: {ex.Message}");
        }
    }
}
