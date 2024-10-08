using Grpc.Core;
using Hightechinnovators.Dtos.Type;
using Hightechinnovators.Dtos.DemodtoV1;
using Google.Protobuf;
using RxDatasets.Abstractions;

namespace RxDatasets.DemodtoV1.Server;

public class WriterServiceImpl(IDatasetWriter<Demodto> dataset) : WriterService.WriterServiceBase
{
    public override async Task<WriteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        try
        {
            return Map(await dataset.Delete(request.Name, context.CancellationToken));
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
            return Map(await dataset.Put(entity.Name, entity.ToByteArray(), context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<WriteResponse> PutAll(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken).Select(vodtitle => (vodtitle.Name, vodtitle.ToByteArray()));
        try
        {
            return Map(await dataset.PutAll(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }
    public override async Task<WriteResponse> PutAllChildren(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken).Select(vodtitle => (vodtitle.Name, vodtitle.ToByteArray()));
        try
        {
            return Map(await dataset.PutAllChildren(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }
    public override async Task<WriteResponse> PutMany(IAsyncStreamReader<Demodto> requestStream, ServerCallContext context)
    {
        var stream = requestStream.ReadAllAsync(context.CancellationToken).Select(vodtitle => (vodtitle.Name, vodtitle.ToByteArray()));
        try
        {
            return Map(await dataset.PutMany(stream, context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }
    private WriteResponse Map(IDatasetWriter<Demodto>.WriteResponse response)
    {
            return new ()
            {
                NewDatasetVersion = response.NewDatasetVersion,
                NumAddedEntities = response.NumAddedEntities,
                NumUpdatedEntities = response.NumUpdatedEntities,
                NumDeletedEntities = response.NumDeletedEntities,
            };
    }
}