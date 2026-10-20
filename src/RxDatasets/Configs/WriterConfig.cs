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

    /// <summary>
    /// When true, concurrent write operations (Put, Delete, PutMany, DeleteMany, PutOrDeleteMany, DeleteChildren)
    /// are coalesced into single delta transactions to reduce storage round-trips.
    /// PutAll and PutAllChildren are excluded because their implicit-delete semantics make merging unsafe.
    /// Opt in via <c>DtoServerBuilder.WithWriteCoalescing()</c>.
    /// </summary>
    public bool EnableWriteCoalescing { get; set; } = false;
}

