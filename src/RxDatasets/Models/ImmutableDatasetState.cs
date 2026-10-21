using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableDatasetState(ImmutableIndexNode<ImmutableSnapshotEntity> Root, ulong SnapshotVersion, ulong LatestVersion)
{
    /// <summary>The hash of the alias definitions the aliases in this state were computed with.</summary>
    public string AliasDefinitionsHash { get; init; } = "";

    /// <summary>
    /// The reverse alias maps: per alias name, alias value → the full name of the entity that owns it. The same
    /// owners as the entities' <see cref="ImmutableSnapshotEntity.Aliases"/>; swapped together with <see cref="Root"/>.
    /// </summary>
    public ImmutableDictionary<string, ImmutableIndexNode<string>> Aliases { get; init; } =
        ImmutableDictionary<string, ImmutableIndexNode<string>>.Empty;

    /// <summary>The entity that owns <paramref name="alias"/> under <paramref name="aliasName"/>, or null.</summary>
    public string? ResolveAlias(string aliasName, string alias) =>
        Aliases.TryGetValue(aliasName, out var map) && map.TryGetValue(alias, out var owner) ? owner : null;

    /// <summary>The alias values under <paramref name="aliasName"/> that equal or start with <paramref name="prefix"/>/.</summary>
    public IReadOnlyList<string> ListAliases(string aliasName, string prefix) =>
        Aliases.TryGetValue(aliasName, out var map) ? map.EnumerateAll(prefix).Select(alias => alias.Key).ToList() : [];

    public static ImmutableDatasetState Empty(ulong snapshotVersion, ulong latestVersion) =>
        new(ImmutableIndexNode<ImmutableSnapshotEntity>.Empty, snapshotVersion, latestVersion);

    public static ImmutableDatasetState FromEntities(IEnumerable<KeyValuePair<string, ImmutableSnapshotEntity>> entities,
        ulong snapshotVersion, ulong latestVersion)
    {
        var builder = ImmutableIndexNode<ImmutableSnapshotEntity>.Empty.ToBuilder();
        foreach (var (name, entity) in entities)
            builder.Set(name, entity);
        return new ImmutableDatasetState(builder.ToImmutable(), snapshotVersion, latestVersion);
    }

    public bool TryGetEntity(string name, out ImmutableSnapshotEntity entity) => Root.TryGetValue(name, out entity);

    public ImmutableSnapshotEntity GetEntity(string name) =>
        Root.TryGetValue(name, out var entity) ? entity : throw new KeyNotFoundException($"Entity {name} is not in the index");

    /// <summary>
    /// All entities, deleted ones included, with their full names; see <see cref="ImmutableIndexNode{TValue}.EnumerateAll"/>.
    /// </summary>
    public IEnumerable<KeyValuePair<string, ImmutableSnapshotEntity>> EnumerateAll(string? prefix = "",
        Func<ImmutableSnapshotEntity, bool>? where = null) => Root.EnumerateAll(prefix, where);

    /// <summary>Reads a snapshot index in the tree format, the legacy flat format, or (in theory) both.</summary>
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var builder = ImmutableIndexNode<ImmutableSnapshotEntity>.Empty.ToBuilder();
        var aliases = new AliasMapsBuilder(ImmutableDictionary<string, ImmutableIndexNode<string>>.Empty);
        if (snapshotIndex.Root != null)
            AddNode(builder, snapshotIndex.Root, aliases, []);
        foreach (var (name, entity) in snapshotIndex.Entities)
            builder.Set(name, ImmutableSnapshotEntity.FromProto(entity));

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.ToImmutable(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion)
        {
            AliasDefinitionsHash = snapshotIndex.AliasDefinitionsHash,
            Aliases = aliases.ToImmutable()
        };
    }

    private static void AddNode(ImmutableIndexNode<ImmutableSnapshotEntity>.Builder builder, SnapshotNode node,
        AliasMapsBuilder aliases, List<string> path)
    {
        foreach (var (template, group) in node.Leaves)
        {
            foreach (var (variable, protoEntity) in group.Entities)
            {
                var entity = ImmutableSnapshotEntity.FromProto(protoEntity);
                builder.SetLeaf(template, variable.AsMemory(), entity);
                if (entity.IsLive && !entity.Aliases.IsEmpty)
                    aliases.Add(IndexKeys.Join(string.Join('/', path), IndexKeys.LeafName(template, variable)), entity.Aliases);
            }
        }

        foreach (var (collection, group) in node.Children)
        {
            foreach (var (id, child) in group.Nodes)
            {
                path.Add(collection);
                path.Add(id);
                AddNode(builder.Child(collection.AsMemory(), id.AsMemory()), child, aliases, path);
                path.RemoveRange(path.Count - 2, 2);
            }
        }
    }

    /// <summary>Writes the tree format only; readers from before the tree format see an empty index.</summary>
    public SnapshotIndex ToProto()
    {
        return new SnapshotIndex
        {
            SnapshotVersionLegacy = 0,
            SnapshotVersion = SnapshotVersion,
            Root = ToProto(Root),
            AliasDefinitionsHash = AliasDefinitionsHash
        };
    }

    private static SnapshotNode ToProto(ImmutableIndexNode<ImmutableSnapshotEntity> node)
    {
        var proto = new SnapshotNode();
        foreach (var (template, group) in node.Leaves)
        {
            var protoGroup = new SnapshotLeafGroup();
            foreach (var (variable, entity) in group)
                protoGroup.Entities.Add(variable.ToString(), entity.ToProto());
            proto.Leaves.Add(template, protoGroup);
        }

        foreach (var (collection, group) in node.Children)
        {
            var protoGroup = new SnapshotChildGroup();
            foreach (var (id, child) in group)
                protoGroup.Nodes.Add(id.ToString(), ToProto(child));
            proto.Children.Add(collection.ToString(), protoGroup);
        }

        return proto;
    }
}

/// <summary>Updates the reverse alias maps of a state, copying only what it touches.</summary>
public sealed class AliasMapsBuilder(ImmutableDictionary<string, ImmutableIndexNode<string>> original)
{
    private readonly Dictionary<string, ImmutableIndexNode<string>.Builder> _builders = new(StringComparer.Ordinal);

    private ImmutableIndexNode<string>.Builder For(string aliasName)
    {
        if (!_builders.TryGetValue(aliasName, out var builder))
        {
            _builders[aliasName] = builder =
                (original.GetValueOrDefault(aliasName) ?? ImmutableIndexNode<string>.Empty).ToBuilder();
        }

        return builder;
    }

    public void Add(string entityName, IEnumerable<(string Name, string Value)> aliases)
    {
        foreach (var (name, value) in aliases)
            For(name).Set(value, entityName);
    }

    /// <summary>Removes the aliases that <paramref name="entityName"/> owns; an alias that moved on is left alone.</summary>
    public void Remove(string entityName, IEnumerable<(string Name, string Value)> aliases)
    {
        foreach (var (name, value) in aliases)
        {
            var map = For(name);
            if (map.Get(value) == entityName)
                map.Remove(value);
        }
    }

    public ImmutableDictionary<string, ImmutableIndexNode<string>> ToImmutable()
    {
        if (_builders.Count == 0)
            return original;

        var result = original.ToBuilder();
        foreach (var (name, builder) in _builders)
            result[name] = builder.ToImmutable();
        return result.ToImmutable();
    }
}
