using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// <see cref="InMemoryDataset{T}.WaitChangedNames"/> must not miss a write that lands
/// while the wait is starting (VLOW-30).
/// </summary>
public class InMemoryDatasetWaitChangedNamesTests
{
    private const int Rounds = 2000;
    private static readonly TimeSpan GiveUpAfter = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Should_return_for_a_write_that_lands_while_the_wait_starts()
    {
        // Given
        var dataset = new InMemoryDataset<object>();
        var lost = 0;

        // When — a waiter and a writer, released together, round after round
        for (var round = 0; round < Rounds; round++)
        {
            var known = await dataset.GetLatestVersion();
            using var start = new Barrier(2);
            var wait = Task.Run(async () =>
            {
                start.SignalAndWait();
                using var giveUp = new CancellationTokenSource(GiveUpAfter);
                try
                {
                    await dataset.WaitChangedNames(known, giveUp.Token);
                    return true;
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            });
            var write = Task.Run(async () =>
            {
                start.SignalAndWait();
                await dataset.Put($"entity-{round}", [1, (byte)round], null, null);
            });

            await write;
            if (!await wait && await dataset.GetLatestVersion() > known)
            {
                lost++;
            }
        }

        // Then — every wait returned: the change was there to be seen
        lost.Should().Be(0, "a wait that started as the write landed must still see it");
    }
}
