using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;

namespace RxDatasets.Aliasing;

public static class AliasingExtensions
{
    /// <summary>
    /// Returns all (aliasName, aliasValue) tuples that the registered alias datasets would produce for the given entity.
    /// Serializes the DTO to protobuf bytes and evaluates every registered alias factory for <typeparamref name="T"/>.
    /// Returns an empty list when no alias datasets are registered for the type.
    /// </summary>
    /// <remarks>
    /// This is the factory output, not the set the entity owns in the index: the two differ only for an entity that
    /// lost an alias in a rebuild after changed definitions. Callers check the shape of the aliases before a write, so
    /// the factory output is what they need; resolve an alias to find its owner.
    /// </remarks>
    /// <typeparam name="T">The DTO type (must be a protobuf message)</typeparam>
    /// <param name="sp">The service provider</param>
    /// <param name="entityId">The entity's storage key (passed to alias factories that need it)</param>
    /// <param name="dto">The DTO instance to evaluate</param>
    public static IReadOnlyList<(string Name, string Value)> GetAliasesFor<T>(
        this IServiceProvider sp,
        string entityId,
        T dto)
        where T : class, IMessage<T>, new()
    {
        var readers = sp.GetServices<AliasDatasetReader<T>>();
        var bytes = new ReadOnlyMemory<byte>(dto.ToByteArray());
        return readers
            .SelectMany(r => r.EvaluateAliases(entityId, bytes))
            .ToList();
    }
}
