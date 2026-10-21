using Grpc.Core;
using Hightechinnovators.Dtos.Demodto2V1;
using Hightechinnovators.Dtos.Type;

namespace RxDatasets.Demodto2V1.Server;

public class ReaderServiceImpl(GrpcReaderServiceImpl<Demodto2> grpcReaderServiceImpl) : ReaderService.ReaderServiceBase
{
    public override async Task<Demodto2> GetDemodto2(GetDemodto2Request request, ServerCallContext context)
    {
        try
        {
            return await grpcReaderServiceImpl.Get(request.Name, context.CancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<BatchGetDemodto2sResponse> BatchGetDemodto2s(BatchGetDemodto2sRequest request, ServerCallContext context)
    {
        try
        {
            var batch = await grpcReaderServiceImpl.BatchGet(request.Names, request.Parent, context.CancellationToken);
            return new BatchGetDemodto2sResponse { Demodto2S = { batch } };
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<ListDemodto2sResponse> ListDemodto2s(ListDemodto2sRequest request, ServerCallContext context)
    {
        try
        {
            var (batch, npt) = await grpcReaderServiceImpl.GetChildren(request.Parent, request.PageSize, request.PageToken, context.CancellationToken);
            var response = new ListDemodto2sResponse
            {
                Demodto2S = { batch }
            };
            if (npt != null)
                response.NextPageToken = npt;
            return response;
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<ListChangesResponse> ListChanges(ListChangesRequest request, ServerCallContext context)
    {
        try
        {
            return Map(await grpcReaderServiceImpl.ListChanges(Map(request), context.CancellationToken));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<CountChildrenResponse> CountChildren(CountChildrenRequest request, ServerCallContext context)
    {
        try
        {
            var count = await grpcReaderServiceImpl.CountChildren(request.Parent, context.CancellationToken);
            return new CountChildrenResponse { Count = count };
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    private ListChangesResponse Map(GrpcReaderServiceImpl<Demodto2>.ListChangesResponse response)
    {
        return new()
        {
            ToDatasetVersion = response.ToDatasetVersion,
            NewEntities = { response.NewEntities },
            ChangedEntities = { response.ChangedEntities },
            DeletedEntities = { response.DeletedEntities },
            NextPageToken = response.NextPageToken,
        };
    }

    private GrpcReaderServiceImpl<Demodto2>.ListChangesRequest Map(ListChangesRequest request)
    {
        return new()
        {
            FromDatasetVersion = request.FromDatasetVersion,
            PageSize = request.PageSize,
            PageToken = request.PageToken,
            WaitForChanges = request.WaitForChanges,
        };
    }
}