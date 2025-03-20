namespace RxDatasets.Tests;

public class PaginationHelperTests
{
    [Fact]
    public void PaginateShouldSupportThatASecondPageCanBeTheRequestedSizeAndFinishTheSequence()
    {
        var allChanges = Enumerable.Range(0, 100).ToList();
        var skipped = 0;
        int defaultPageSize = 50;
        int maxPageSize = 100;

        var (pageOfChanges, _, endVersion) = PaginationHelper.Paginate(allChanges, 0, 0, 1, defaultPageSize, maxPageSize,
            (key, collection, alreadyReadSize, toDatasetVersion) => { skipped = alreadyReadSize; });

        pageOfChanges.Should().HaveCount(defaultPageSize);
        pageOfChanges[0].Should().Be(0);
        skipped.Should().Be(defaultPageSize);
        endVersion.Should().Be(0);

        int? skipped2 = null;
        var (page2OfChanges, _, endVersion2) = PaginationHelper.Paginate(allChanges, 0, skipped, 1, defaultPageSize, maxPageSize,
            (key, collection, alreadyReadSize, toDatasetVersion) => { skipped2 = alreadyReadSize; });

        page2OfChanges.Should().HaveCount(50);
        page2OfChanges[0].Should().Be(50);
        skipped2.Should().BeNull();
        endVersion2.Should().Be(1);
    }

    [Fact]
    public void PaginateShouldSupportThatASecondPageSmallerThanPageSize()
    {
        var allChanges = Enumerable.Range(0, 75).ToList();
        var skipped = 0;
        int defaultPageSize = 50;
        int maxPageSize = 100;

        var (pageOfChanges, _, endVersion) = PaginationHelper.Paginate(allChanges, 0, 0, 1, defaultPageSize, maxPageSize,
            (key, collection, alreadyReadSize, toDatasetVersion) => { skipped = alreadyReadSize; });

        pageOfChanges.Should().HaveCount(defaultPageSize);
        skipped.Should().Be(defaultPageSize);
        endVersion.Should().Be(0);

        int? skipped2 = null;
        var (page2OfChanges, _, endVersion2) = PaginationHelper.Paginate(allChanges, 0, skipped, 1, defaultPageSize, maxPageSize,
            (key, collection, alreadyReadSize, toDatasetVersion) => { skipped2 = alreadyReadSize; });

        page2OfChanges.Should().HaveCount(25);
        page2OfChanges[0].Should().Be(50);
        skipped2.Should().BeNull();
        endVersion2.Should().Be(1);
    }

    [Fact]
    public void PaginateShouldSupportThatAFirstPageSmallerThanPageSize()
    {
        var allChanges = Enumerable.Range(0, 25).ToList();
        int? skipped = null;
        int defaultPageSize = 50;
        int maxPageSize = 100;

        var (pageOfChanges, _, endVersion) = PaginationHelper.Paginate(allChanges, 0, 0, 1, defaultPageSize, maxPageSize,
            (key, collection, alreadyReadSize, toDatasetVersion) => { skipped = alreadyReadSize; });

        pageOfChanges.Should().HaveCount(25);
        skipped.Should().BeNull();
        endVersion.Should().Be(1);
    }
}
