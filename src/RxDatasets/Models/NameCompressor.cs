using System.Text;

namespace RxDatasets.Models;

/// <summary>
/// Compresses entity names by stripping fixed (literal) path segments defined by an ID pattern,
/// keeping only the variable (*) segments. This dramatically reduces trie depth and node count
/// for hierarchical entity names with repeating structural segments.
///
/// Example pattern: "t/*/s/*/items/*/values"
///   Fixed positions: 0 ("t"), 2 ("s"), 4 ("items"), 6 ("values")
///   Variable positions: 1, 3, 5
///   "t/abc/s/def/items/001/values" → "abc/def/001"
/// </summary>
public sealed class NameCompressor
{
    private readonly string[] _patternSegments;
    private readonly int[] _variablePositions;
    private readonly bool[] _isVariable;
    private readonly int _totalSegments;

    private NameCompressor(string[] patternSegments, int[] variablePositions, bool[] isVariable)
    {
        _patternSegments = patternSegments;
        _variablePositions = variablePositions;
        _isVariable = isVariable;
        _totalSegments = patternSegments.Length;
    }

    /// <summary>
    /// Creates a NameCompressor from an ID pattern string.
    /// Pattern segments that are "*" are variable; all others are fixed literals.
    /// Returns null if the pattern is null/empty or has no fixed segments to strip.
    /// </summary>
    public static NameCompressor? FromPattern(string? pattern)
    {
        if (string.IsNullOrWhiteSpace(pattern))
            return null;

        var segments = pattern.Split('/');
        if (segments.Length == 0)
            return null;

        var isVariable = new bool[segments.Length];
        var variablePositions = new List<int>();
        bool hasFixed = false;

        for (int i = 0; i < segments.Length; i++)
        {
            if (segments[i] == "*")
            {
                isVariable[i] = true;
                variablePositions.Add(i);
            }
            else
            {
                hasFixed = true;
            }
        }

        if (!hasFixed || variablePositions.Count == 0)
            return null;

        return new NameCompressor(segments, variablePositions.ToArray(), isVariable);
    }

    /// <summary>
    /// Number of variable segments in the pattern.
    /// </summary>
    public int VariableSegmentCount => _variablePositions.Length;

    /// <summary>
    /// Compresses a full entity name by extracting only the variable segments.
    /// "t/abc/s/def/items/001/values" → "abc/def/001"
    /// </summary>
    public string Compress(string fullName)
    {
        // Fast path: extract variable segments from the full name
        var segments = SplitSegments(fullName);

        if (segments.Length != _totalSegments)
            return fullName; // Name doesn't match pattern; return as-is

        if (_variablePositions.Length == 1)
            return segments[_variablePositions[0]];

        var sb = new StringBuilder();
        for (int i = 0; i < _variablePositions.Length; i++)
        {
            if (i > 0) sb.Append('/');
            sb.Append(segments[_variablePositions[i]]);
        }

        return sb.ToString();
    }

    /// <summary>
    /// Expands a compressed name back to the full name by interleaving fixed segments.
    /// "abc/def/001" → "t/abc/s/def/items/001/values"
    /// </summary>
    public string Expand(string compressedName)
    {
        var varSegments = SplitSegments(compressedName);

        if (varSegments.Length != _variablePositions.Length)
            return compressedName; // Doesn't match expected variable count; return as-is

        var sb = new StringBuilder();
        int varIdx = 0;

        for (int i = 0; i < _totalSegments; i++)
        {
            if (i > 0) sb.Append('/');

            if (_isVariable[i])
            {
                sb.Append(varSegments[varIdx++]);
            }
            else
            {
                sb.Append(_patternSegments[i]);
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Compresses a partial prefix (for ListDescendants / NavigateTo).
    /// Only segments that fall within the prefix length are processed;
    /// fixed segments are stripped, variable segments are kept.
    /// "t/abc/s/def/items" (5 segments) → "abc/def"
    /// "t/abc/s" (3 segments) → "abc"
    /// "" → ""
    /// </summary>
    public string CompressPrefix(string fullPrefix)
    {
        if (fullPrefix.Length == 0)
            return "";

        var segments = SplitSegments(fullPrefix);

        if (segments.Length > _totalSegments)
            return fullPrefix; // Longer than pattern; return as-is

        var sb = new StringBuilder();
        bool first = true;

        for (int i = 0; i < segments.Length; i++)
        {
            if (i < _isVariable.Length && _isVariable[i])
            {
                if (!first) sb.Append('/');
                sb.Append(segments[i]);
                first = false;
            }
            // Fixed segments are skipped
        }

        return sb.ToString();
    }

    /// <summary>
    /// Expands a compressed prefix back to a full prefix.
    /// "abc/def" → "t/abc/s/def/items"
    /// "abc" → "t/abc/s"
    /// "" → ""
    /// </summary>
    public string ExpandPrefix(string compressedPrefix)
    {
        if (compressedPrefix.Length == 0)
            return "";

        var varSegments = SplitSegments(compressedPrefix);

        // Find how far into the pattern these variable segments reach
        int varIdx = 0;
        int lastPatternPos = -1;

        for (int i = 0; i < _totalSegments && varIdx < varSegments.Length; i++)
        {
            if (_isVariable[i])
            {
                varIdx++;
                lastPatternPos = i;
            }
        }

        if (lastPatternPos < 0)
            return compressedPrefix;

        // Reconstruct prefix up to lastPatternPos, then append trailing fixed segments
        int expandTo = lastPatternPos;

        // Include any trailing fixed segments after the last variable
        for (int i = lastPatternPos + 1; i < _totalSegments; i++)
        {
            if (_isVariable[i])
                break;
            expandTo = i;
        }

        var sb = new StringBuilder();
        varIdx = 0;

        for (int i = 0; i <= expandTo; i++)
        {
            if (i > 0) sb.Append('/');

            if (_isVariable[i])
            {
                sb.Append(varSegments[varIdx++]);
            }
            else
            {
                sb.Append(_patternSegments[i]);
            }
        }

        return sb.ToString();
    }

    private static string[] SplitSegments(string name)
    {
        return name.Split('/');
    }
}
