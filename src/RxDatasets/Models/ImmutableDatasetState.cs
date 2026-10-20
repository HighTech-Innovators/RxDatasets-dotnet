namespace RxDatasets.Models;

public record ImmutableDatasetState(ImmutableIndexNode<ImmutableSnapshotEntity> Root, ulong SnapshotVersion, ulong LatestVersion)
{
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

    /// <summary>All entities, deleted ones included, with their full names; see <see cref="ImmutableIndexNode{TValue}.EnumerateAll"/>.</summary>
    public IEnumerable<KeyValuePair<string, ImmutableSnapshotEntity>> EnumerateAll(string prefix = "",
        Func<ImmutableSnapshotEntity, bool>? where = null) => Root.EnumerateAll(prefix, where);

    /// <summary>Reads a snapshot index in the tree format, the legacy flat format, or (in theory) both.</summary>
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var builder = ImmutableIndexNode<ImmutableSnapshotEntity>.Empty.ToBuilder();
        if (snapshotIndex.Root != null)
            AddNode(builder, snapshotIndex.Root);
        foreach (var (name, entity) in snapshotIndex.Entities)
            builder.Set(name, ImmutableSnapshotEntity.FromProto(entity));

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.ToImmutable(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }

    private static void AddNode(ImmutableIndexNode<ImmutableSnapshotEntity>.Builder builder, SnapshotNode node)
    {
        foreach (var (template, group) in node.Leaves)
        {
            foreach (var (variable, entity) in group.Entities)
                builder.SetLeaf(template, variable.AsMemory(), ImmutableSnapshotEntity.FromProto(entity));
        }

        foreach (var (collection, group) in node.Children)
        {
            foreach (var (id, child) in group.Nodes)
                AddNode(builder.Child(collection.AsMemory(), id.AsMemory()), child);
        }
    }

    /// <summary>Writes the tree format only; readers from before the tree format see an empty index.</summary>
    public SnapshotIndex ToProto()
    {
        return new SnapshotIndex
        {
            SnapshotVersionLegacy = 0,
            SnapshotVersion = SnapshotVersion,
            Root = ToProto(Root)
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
