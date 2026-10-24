using System.Security.Cryptography;
using System.Text;

namespace RxDatasets.Configs;

/// <summary>
/// An alias name of a dataset. <see cref="Factory"/> gets the entity name and its bytes, returns the entity's alias
/// values under <see cref="Name"/>, and throws on a payload it can't evaluate. <see cref="DefinitionHash"/> changes
/// whenever the factory would give different values, so the stored aliases are recomputed.
/// </summary>
public record AliasDefinition(string Name, Func<string, ReadOnlyMemory<byte>, IEnumerable<string>> Factory,
    string DefinitionHash);

/// <summary>The alias definitions of the dataset of <typeparamref name="T"/>, read by its writer.</summary>
public class AliasOptions<T>
{
    public List<AliasDefinition> Definitions { get; } = [];

    /// <summary>One hash over all definitions, independent of their order; empty without definitions.</summary>
    public static string HashOf(IEnumerable<AliasDefinition> definitions)
    {
        var lines = definitions.Select(d => $"{d.Name}={d.DefinitionHash}").Order(StringComparer.Ordinal).ToList();
        return lines.Count == 0
            ? ""
            : Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', lines))));
    }
}
