using Grpc.Core;
using Hightechinnovators.Dtos.TestdtoV1;
using Hightechinnovators.Dtos.Type;

namespace RxDatasets.TestdtoV1.Server;

public class ReaderServiceImpl(GrpcReaderServiceImpl<Testdto> grpcReaderServiceImpl) : ReaderService.ReaderServiceBase
{
    public override async Task<Testdto> GetTestdto(GetTestdtoRequest request, ServerCallContext context)
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

    public override async Task<BatchGetTestdtosResponse> BatchGetTestdtos(BatchGetTestdtosRequest request, ServerCallContext context)
    {
        try
        {
            var batch = await grpcReaderServiceImpl.BatchGet(request.Names, request.Parent, context.CancellationToken);
            return new BatchGetTestdtosResponse { Testdtos = { batch } };
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }

    public override async Task<ListTestdtosResponse> ListTestdtos(ListTestdtosRequest request, ServerCallContext context)
    {
        try
        {
            var (batch, npt) = await grpcReaderServiceImpl.GetChildren(request.Parent, request.PageSize, request.PageToken, context.CancellationToken);
            var response = new ListTestdtosResponse
            {
                Testdtos = { batch }
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

    private ListChangesResponse Map(GrpcReaderServiceImpl<Testdto>.ListChangesResponse response)
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

    private GrpcReaderServiceImpl<Testdto>.ListChangesRequest Map(ListChangesRequest request)
    {
        return new()
        {
            FromDatasetVersion = request.FromDatasetVersion,
            PageSize = request.PageSize,
            PageToken = request.PageToken,
        };
    }
}
