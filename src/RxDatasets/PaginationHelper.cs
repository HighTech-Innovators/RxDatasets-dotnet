namespace RxDatasets;

public static class PaginationHelper
{
    public static (List<T> Changes, string NextPageToken, ulong ResponseToDatasetVersion) Paginate<T>(List<T> allChanges,
        int requestPageSize, int skipSize, ulong toDatasetVersion, int defaultPageSize, int maxPageSize,
        Action<string, List<T>, int, ulong> storeRemainder)
    {
        if (requestPageSize == 0)
            requestPageSize = defaultPageSize;

        if (requestPageSize > maxPageSize)
            requestPageSize = maxPageSize;

        var readExclusiveUpperBound = skipSize + requestPageSize;
        if (readExclusiveUpperBound > allChanges.Count)
            readExclusiveUpperBound = allChanges.Count;

        var thisPage = allChanges[skipSize..readExclusiveUpperBound];

        if (readExclusiveUpperBound == allChanges.Count)
            return (thisPage, string.Empty, toDatasetVersion);

        var nextPageToken = Guid.NewGuid().ToString().ToLowerInvariant();
        storeRemainder(nextPageToken, allChanges, readExclusiveUpperBound, toDatasetVersion);
        return (thisPage, nextPageToken, 0);
    }
}
