using System.Text.RegularExpressions;

namespace HTI.RxDatasets;

public class SharedMethods
{
    public static int ExtractNumber(string path)
    {
        // The number is assumed to be the version number in the key
        var match = Regex.Match(path, @"(\d+)/");
        return match.Success ? int.Parse(match.Groups[1].Value) : 0;
    }
}