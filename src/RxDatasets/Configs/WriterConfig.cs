using System.Text.RegularExpressions;

namespace RxDatasets.Configs;

public class WriterConfig
{
    private string _allowedNamesPattern = "";
    private Regex? _allowedNamesRegex;

    public string AllowedNamesPattern
    {
        get => _allowedNamesPattern;
        set
        {
            _allowedNamesPattern = value;
            _allowedNamesRegex = string.IsNullOrEmpty(value) ? null : new Regex(value, RegexOptions.Compiled);
        }
    }

    public Regex? AllowedNamesRegex => _allowedNamesRegex;
}

