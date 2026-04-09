namespace RxDatasets;

/// <summary>
/// Optional configuration for <see cref="GrpcReaderServiceImpl{TEntity}"/>.
/// Register as a singleton in DI to enable features like entity ID injection.
/// </summary>
/// <param name="EntityIdSetter">
/// When provided, called after every <c>ParseFrom</c> to set the entity's ID from the storage key (entity name).
/// This enables stripping the ID field from stored protobuf bytes to save space,
/// since the ID is always known from the storage index.
/// Example: <c>(entity, name) =&gt; entity.Id = name</c>
/// </param>
public record GrpcReaderConfig<TEntity>(Action<TEntity, string>? EntityIdSetter = null);
