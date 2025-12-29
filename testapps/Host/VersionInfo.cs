using System.Reflection;

namespace Host;

/// <summary>
/// Utility class for retrieving version information from the assembly.
/// </summary>
public static class VersionInfo
{
    private static string? _informationalVersion;

    /// <summary>
    /// Gets the informational version of the application.
    /// </summary>
    public static string GetVersion()
    {
        if (_informationalVersion != null)
            return _informationalVersion;

        var assembly = typeof(VersionInfo).Assembly;
        var version = assembly.GetName().Version?.ToString() ?? "unknown";
        
        var informationalVersionAttr = assembly.GetCustomAttributes(false)
            .OfType<AssemblyInformationalVersionAttribute>()
            .FirstOrDefault();
        
        _informationalVersion = informationalVersionAttr?.InformationalVersion ?? version;
        return _informationalVersion;
    }
}
