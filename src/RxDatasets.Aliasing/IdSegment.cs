using System.Buffers;
using System.Text;

namespace RxDatasets.Aliasing;

/// <summary>
/// Encodes a raw value (one that may contain <c>/</c> or any other character) as one id segment.
/// </summary>
public static class IdSegment
{
    private const string HexDigits = "0123456789abcdef";

    private static readonly SearchValues<char> Unreserved =
        SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._");

    /// <summary>
    /// Every UTF-8 byte outside <c>A-Z a-z 0-9 - . _</c> (so <c>~</c> too) becomes <c>~</c> plus two lowercase hex
    /// digits. Not idempotent: pass raw values only, never an encoded one (<c>~2f</c> becomes <c>~7e2f</c>).
    /// </summary>
    public static string Encode(string value)
    {
        var first = value.AsSpan().IndexOfAnyExcept(Unreserved);
        if (first < 0)
            return value;

        var length = first;
        var rest = value.AsSpan(first);
        while (!rest.IsEmpty)
        {
            Rune.DecodeFromUtf16(rest, out var rune, out var consumed);
            length += 3 * rune.Utf8SequenceLength;
            rest = rest[consumed..];

            var safe = SafeRunLength(rest);
            length += safe;
            rest = rest[safe..];
        }

        return string.Create(length, value, static (chars, value) =>
        {
            Span<byte> utf8 = stackalloc byte[4];
            var rest = value.AsSpan();
            while (true)
            {
                var safe = SafeRunLength(rest);
                rest[..safe].CopyTo(chars);
                chars = chars[safe..];
                rest = rest[safe..];
                if (rest.IsEmpty)
                    return;

                Rune.DecodeFromUtf16(rest, out var rune, out var consumed);
                rest = rest[consumed..];
                foreach (var b in utf8[..rune.EncodeToUtf8(utf8)])
                {
                    chars[0] = '~';
                    chars[1] = HexDigits[b >> 4];
                    chars[2] = HexDigits[b & 0xF];
                    chars = chars[3..];
                }
            }
        });
    }

    private static int SafeRunLength(ReadOnlySpan<char> chars)
    {
        var index = chars.IndexOfAnyExcept(Unreserved);
        return index < 0 ? chars.Length : index;
    }
}