using System.Text;

namespace RxDatasets.Aliasing;

/// <summary>
/// Encodes a raw value (one that may contain <c>/</c> or any other character) as one id segment.
/// </summary>
public static class IdSegment
{
    /// <summary>
    /// Every UTF-8 byte outside <c>A-Z a-z 0-9 - . _</c> becomes <c>~</c> plus two lowercase hex digits. A <c>~xx</c>
    /// with lowercase hex is already encoded and kept as is, so encoding is idempotent; <c>~2F</c> becomes
    /// <c>~7e2F</c>.
    /// </summary>
    public static string Encode(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var sb = new StringBuilder(bytes.Length);
        for (var i = 0; i < bytes.Length; i++)
        {
            var c = (char)bytes[i];
            if (char.IsAsciiLetterOrDigit(c) || c is '-' or '.' or '_')
                sb.Append(c);
            else if (c == '~' && i + 2 < bytes.Length
                     && char.IsAsciiHexDigitLower((char)bytes[i + 1]) && char.IsAsciiHexDigitLower((char)bytes[i + 2]))
                sb.Append(c).Append((char)bytes[++i]).Append((char)bytes[++i]);
            else
                sb.Append('~').Append(bytes[i].ToString("x2"));
        }

        return sb.ToString();
    }
}
