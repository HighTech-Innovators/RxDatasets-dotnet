using System.Text;

namespace RxDatasets.Aliasing;

/// <summary>
/// Encodes a raw value (one that may contain <c>/</c> or any other character) as one id segment.
/// </summary>
public static class IdSegment
{
    /// <summary>
    /// Every UTF-8 byte outside <c>A-Z a-z 0-9 - . _</c> (so <c>~</c> too) becomes <c>~</c> plus two lowercase hex
    /// digits. Not idempotent: pass raw values only, never an encoded one (<c>~2f</c> becomes <c>~7e2f</c>).
    /// </summary>
    public static string Encode(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var sb = new StringBuilder(bytes.Length);
        foreach (var b in bytes)
        {
            var c = (char)b;
            if (char.IsAsciiLetterOrDigit(c) || c is '-' or '.' or '_')
                sb.Append(c);
            else
                sb.Append('~').Append(b.ToString("x2"));
        }

        return sb.ToString();
    }
}
