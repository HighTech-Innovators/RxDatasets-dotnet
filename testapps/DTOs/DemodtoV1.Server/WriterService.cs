using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Hightechinnovators.Dtos.Type;
using RxDatasets;
using RxDatasets.Abstractions;

namespace RxDatasets.DemodtoV1.Server;

public class WriterServiceImpl(IDatasetWriter<Demodto> dataset) : WriterService.WriterServiceBase
{
    public override async Task<WriteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        try
        {
            var response = await dataset.Delete(request.Name, context.CancellationToken);
            return Map(response, context);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> Put(Demodto entity, ServerCallContext context)
    {
        try
        {
            var response = await dataset.Put(entity.Name, entity.ToByteArray(), context.CancellationToken);
            return Map(response, context, entity.Name);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAll(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var entities = new List<(string name, byte[] bytes)>();
        await foreach (var item in requestStream.ReadAllAsync(context.CancellationToken))
        {
            entities.Add((item.Name, item.ToByteArray()));
        }
        
        try
        {
            var response = await dataset.PutAll(entities.Select(e => (e.name, e.bytes)).ToAsyncEnumerable(), context.CancellationToken);
            return Map(response, context, entities.Select(e => e.name));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAllChildren(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var entities = new List<(string name, byte[] bytes)>();
        await foreach (var item in requestStream.ReadAllAsync(context.CancellationToken))
        {
            entities.Add((item.Name, item.ToByteArray()));
        }
        
        try
        {
            var response = await dataset.PutAllChildren(entities.Select(e => (e.name, e.bytes)).ToAsyncEnumerable(), context.CancellationToken);
            return Map(response, context, entities.Select(e => e.name));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutMany(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var entities = new List<(string name, byte[] bytes)>();
        await foreach (var item in requestStream.ReadAllAsync(context.CancellationToken))
        {
            entities.Add((item.Name, item.ToByteArray()));
        }
        
        try
        {
            var response = await dataset.PutMany(entities.Select(e => (e.name, e.bytes)).ToAsyncEnumerable(), context.CancellationToken);
            return Map(response, context, entities.Select(e => e.name));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutOrDeleteMany(IAsyncStreamReader<PutOrDelete> requestStream, ServerCallContext context)
    {
        var changes = new List<(string name, byte[]? bytes)>();
        await foreach (var item in requestStream.ReadAllAsync(context.CancellationToken))
        {
            changes.Add((item.Name, item.IsDelete ? (byte[]?)null : item.EntityBytes.ToByteArray()));
        }
        
        try
        {
            var response = await dataset.PutOrDeleteMany(changes.Select(c => c).ToAsyncEnumerable(), context.CancellationToken);
            return Map(response, context, changes.Select(c => c.name));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    /// <summary>
    /// Maps internal WriteResponse to proto WriteResponse, optionally including detailed changes if X-Debug header is present.
    /// </summary>
    private WriteResponse Map(
        IDatasetWriter<Demodto>.WriteResponse response,
        ServerCallContext context,
        string? singleEntityName = null)
    {
        return Map(response, context, singleEntityName != null ? new[] { singleEntityName } : Enumerable.Empty<string>());
    }

    /// <summary>
    /// Maps internal WriteResponse to proto WriteResponse, optionally including detailed changes if X-Debug header is present.
    /// Populates detailed_changes based on the entity names that were involved in the operation.
    /// </summary>
    private WriteResponse Map(
        IDatasetWriter<Demodto>.WriteResponse response,
        ServerCallContext context,
        IEnumerable<string> entityNames)
    {
        var protoResponse = new WriteResponse()
        {
            NewDatasetVersion = response.NewDatasetVersion,
            NumAddedEntities = response.NumAddedEntities,
            NumUpdatedEntities = response.NumUpdatedEntities,
            NumDeletedEntities = response.NumDeletedEntities,
        };

        // Check for X-Debug header (case-insensitive)
        if (!HeaderDetection.HasXDebugHeader(context))
            return protoResponse;

        // Populate detailed_changes when X-Debug header is present
        // Note: In production, this should come from tracking the actual Delta applied.
        // For now, we create placeholder entries based on the entity names and counts.
        // The actual change types would be determined by comparing before/after state.
        var numAdded = (int)response.NumAddedEntities;
        var numUpdated = (int)response.NumUpdatedEntities;
        var numDeleted = (int)response.NumDeletedEntities;
        
        var entityList = entityNames.ToList();
        int addIndex = 0, updateIndex = 0, deleteIndex = 0;

        foreach (var entityName in entityList)
        {
            Hightechinnovators.Dtos.Type.ChangeType changeType;
            
            // Distribute entities across change types based on counts
            if (addIndex < numAdded)
            {
                changeType = Hightechinnovators.Dtos.Type.ChangeType.New;
                addIndex++;
            }
            else if (updateIndex < numUpdated)
            {
                changeType = Hightechinnovators.Dtos.Type.ChangeType.Changed;
                updateIndex++;
            }
            else if (deleteIndex < numDeleted)
            {
                changeType = Hightechinnovators.Dtos.Type.ChangeType.Deleted;
                deleteIndex++;
            }
            else
            {
                // Entity wasn't changed, skip it
                continue;
            }

            protoResponse.DetailedChanges.Add(new DetailedChange
            {
                EntityId = entityName,
                ChangeType = changeType
            });
        }

        return protoResponse;
    }
}
