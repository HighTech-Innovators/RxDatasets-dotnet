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
            bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
            var response = await dataset.Delete(request.Name, includeDetailedChanges, context.CancellationToken);
            return Map(response);
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
            bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
            var response = await dataset.Put(entity.Name, entity.ToByteArray(), includeDetailedChanges, context.CancellationToken);
            return Map(response);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAll(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
        var entities = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(item => (item.Name, item.ToByteArray()));

        try
        {
            var response = await dataset.PutAll(entities, includeDetailedChanges, context.CancellationToken);
            return Map(response);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAllChildren(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
        var entities = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(item => (item.Name, item.ToByteArray()));

        try
        {
            var response = await dataset.PutAllChildren(entities, includeDetailedChanges, context.CancellationToken);
            return Map(response);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutMany(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
        var entities = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(item => (item.Name, item.ToByteArray()));

        try
        {
            var response = await dataset.PutMany(entities, includeDetailedChanges, context.CancellationToken);
            return Map(response);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutOrDeleteMany(IAsyncStreamReader<PutOrDelete> requestStream, ServerCallContext context)
    {
        bool includeDetailedChanges = HeaderDetection.HasXDebugHeader(context);
        var changes = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(item => (item.Name, item.IsDelete ? (byte[]?)null : item.EntityBytes.ToByteArray()));

        try
        {
            var response = await dataset.PutOrDeleteMany(changes, includeDetailedChanges, context.CancellationToken);
            return Map(response);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    /// <summary>
    /// Maps internal WriteResponse to proto WriteResponse, using the actual DetailedChanges from the response.
    /// </summary>
    private WriteResponse Map(IDatasetWriter<Demodto>.WriteResponse response)
    {
        var protoResponse = new WriteResponse()
        {
            NewDatasetVersion = response.NewDatasetVersion,
            NumAddedEntities = response.NumAddedEntities,
            NumUpdatedEntities = response.NumUpdatedEntities,
            NumDeletedEntities = response.NumDeletedEntities,
        };

        // If DetailedChanges were requested and populated, map them to the proto response
        if (response.DetailedChanges != null)
        {
            foreach (var change in response.DetailedChanges)
            {
                protoResponse.DetailedChanges.Add(new Hightechinnovators.Dtos.Type.DetailedChange
                {
                    EntityId = change.EntityId,
                    ChangeType = (Hightechinnovators.Dtos.Type.ChangeType)(int)change.ChangeType
                });
            }
        }

        return protoResponse;
    }}