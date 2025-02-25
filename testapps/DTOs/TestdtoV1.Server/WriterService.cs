using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.TestdtoV1;
using Hightechinnovators.Dtos.Type;
using RxDatasets.Abstractions;

namespace RxDatasets.TestdtoV1.Server;

public class WriterServiceImpl(IDatasetWriter<Testdto> dataset) : WriterService.WriterServiceBase
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

    public override async Task<WriteResponse> Put(Testdto entity, ServerCallContext context)
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

    public override async Task<WriteResponse> PutAll(IAsyncStreamReader<Testdto> requestStream, ServerCallContext context)
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

    private WriteResponse Map(IDatasetWriter<Testdto>.WriteResponse response)
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
