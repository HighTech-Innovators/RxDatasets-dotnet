namespace RxDatasets.Tests;

public class SemaphoreSlimExtensionsTests
{
    [Fact]
    public async Task LockAsync_ShouldHoldTheSemaphore_UntilDisposed()
    {
        // Arrange
        using var semaphore = new SemaphoreSlim(1);

        // Act
        IDisposable slot = await semaphore.LockAsync();
        int whileHeld = semaphore.CurrentCount;
        slot.Dispose();

        // Assert
        whileHeld.Should().Be(0);
        semaphore.CurrentCount.Should().Be(1);
    }

    [Fact]
    public void Lock_ShouldHoldTheSemaphore_UntilDisposed()
    {
        // Arrange
        using var semaphore = new SemaphoreSlim(1);

        // Act
        IDisposable slot = semaphore.Lock();
        int whileHeld = semaphore.CurrentCount;
        slot.Dispose();

        // Assert
        whileHeld.Should().Be(0);
        semaphore.CurrentCount.Should().Be(1);
    }

    [Fact]
    public async Task DisposingTwice_ShouldReleaseOnlyOnce()
    {
        // Arrange: new SemaphoreSlim(1) has no maximum of 1, so a second release would let two holders in
        using var semaphore = new SemaphoreSlim(1);
        IDisposable asyncSlot = await semaphore.LockAsync();
        asyncSlot.Dispose();
        IDisposable syncSlot = semaphore.Lock();
        syncSlot.Dispose();

        // Act
        asyncSlot.Dispose();
        syncSlot.Dispose();

        // Assert
        semaphore.CurrentCount.Should().Be(1);
    }

    [Fact]
    public async Task LockAsync_ShouldThrow_WhenCancelledWhileWaiting()
    {
        // Arrange
        using var semaphore = new SemaphoreSlim(1);
        using IDisposable held = await semaphore.LockAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act
        Func<Task> act = () => semaphore.LockAsync(cts.Token);

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();
        semaphore.CurrentCount.Should().Be(0, "the lock is still held by the first caller");
    }

    [Fact]
    public void Lock_ShouldThrow_WhenCancelledWhileWaiting()
    {
        // Arrange
        using var semaphore = new SemaphoreSlim(1);
        using IDisposable held = semaphore.Lock();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act
        Action act = () => semaphore.Lock(cts.Token);

        // Assert
        act.Should().Throw<OperationCanceledException>();
        semaphore.CurrentCount.Should().Be(0, "the lock is still held by the first caller");
    }
}
