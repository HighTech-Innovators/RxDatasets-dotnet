using System.Text.RegularExpressions;

namespace RxDatasets;

public static partial class SharedMethods
{
    private static readonly Regex LastNumberInPathRegex = LastNumberInPathRegexAttribute();
    public static int ExtractNumber(string path)
    {
        // The number is assumed to be the version number in the key    
        var match = LastNumberInPathRegex.Match(path);  
        return match.Success ? int.Parse(match.Groups[1].Value) : 0;
    }

    [GeneratedRegex(@"/(\d+)/[^/]*$", RegexOptions.Compiled)]
    private static partial Regex LastNumberInPathRegexAttribute();
}