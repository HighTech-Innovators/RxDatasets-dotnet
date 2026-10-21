using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.Demodto2V1;
using Hightechinnovators.Dtos.Type;
using RxDatasets.Abstractions;

namespace RxDatasets.Demodto2V1.Server;

public class WriterServiceImpl(IDatasetWriter<Demodto2> dataset) : WriterService.WriterServiceBase
{
    public override async Task<WriteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        try
        {
            return Map(await dataset.Delete(request.Name, null, null, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> Put(Demodto2 entity, ServerCallContext context)
    {
        try
        {
            return Map(await dataset.Put(entity.Name, entity.ToByteArray(), null, null, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAll(IAsyncStreamReader<Demodto2> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(entity => (entity.Name, entity.ToByteArray(), (string?)null, (DateTimeOffset?)null));
        try
        {
            return Map(await dataset.PutAll(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAllChildren(IAsyncStreamReader<Demodto2> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(entity => (entity.Name, entity.ToByteArray(), (string?)null, (DateTimeOffset?)null));
        try
        {
            return Map(await dataset.PutAllChildren(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutMany(IAsyncStreamReader<Demodto2> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(entity => (entity.Name, entity.ToByteArray(), (string?)null, (DateTimeOffset?)null));
        try
        {
            return Map(await dataset.PutMany(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutOrDeleteMany(IAsyncStreamReader<PutOrDelete> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken)
            .Select(entity => (entity.Name, entity.IsDelete ? (byte[]?)null : entity.EntityBytes.ToByteArray(),
                (string?)null, (DateTimeOffset?)null));
        try
        {
            return Map(await dataset.PutOrDeleteMany(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    private WriteResponse Map(IDatasetWriter<Demodto2>.WriteResponse response)
    {
        return new()
        {
            NewDatasetVersion = response.NewDatasetVersion,
            NumAddedEntities = response.NumAddedEntities,
            NumUpdatedEntities = response.NumUpdatedEntities,
            NumDeletedEntities = response.NumDeletedEntities,
        };
    }
}