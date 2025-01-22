using System.Text.RegularExpressions;

namespace RxDatasets;

public class SharedMethods
{
    private static readonly Regex LastNumberInPathRegex = new(@"/(\d+)/[^/]*$", RegexOptions.Compiled);
    public static int ExtractNumber(string path)
    {
        // The number is assumed to be the version number in the key    
        var match = LastNumberInPathRegex.Match(path);  
        return match.Success ? int.Parse(match.Groups[1].Value) : 0;
    }
}