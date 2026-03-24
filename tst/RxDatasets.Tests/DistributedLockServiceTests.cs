using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using Xunit;

namespace RxDatasets.Tests;

public class DistributedLockServiceTests : IDisposable
{
    private readonly ILockProvider _lockProviderMock;
    private readonly IDiskStorage<TestEntity> _storageMock;
    private readonly INotificationsHub<TestEntity> _notificationsHubMock;
    private readonly ILogger<DistributedLockService<TestEntity>> _loggerMock;
    private readonly StorageConfig _storageConfig;
    private readonly DistributedLockOptions _lockOptions;
    private readonly List<DistributedLockNotification> _notifications;
    private readonly CancellationTokenSource _testCts;
    private readonly TestScheduler _scheduler;

    public DistributedLockServiceTests()
    {
        _lockProviderMock = Substitute.For<ILockProvider>();
        _storageMock = Substitute.For<IDiskStorage<TestEntity>>();
        _notificationsHubMock = Substitute.For<INotificationsHub<TestEntity>>();
        _loggerMock = Substitute.For<ILogger<DistributedLockService<TestEntity>>>();
        _storageConfig = new StorageConfig { DtoName = "TestEntity" };
        _lockOptions = new DistributedLockOptions(); // use defaults
        _notifications = new List<DistributedLockNotification>();
        _testCts = new CancellationTokenSource();

        _notificationsHubMock.Notify(Arg.Any<DistributedLockNotification>())
                .Returns(Task.CompletedTask)
                .AndDoes(call => _notifications.Add(call.Arg<DistributedLockNotification>()));

        _scheduler = new TestScheduler();
        // Nudge virtual time away from DateTimeOffset.MinValue to allow negative offsets safely
        _scheduler.AdvanceBy(TimeSpan.FromHours(1).Ticks);

        // Default: request lock and delete succeed, shouldYield returns false
        _lockProviderMock.RequestLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        _lockProviderMock.DeleteRequestLockAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(false);
        _lockProviderMock.ReleaseLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
    }

    [Fact]
    public async Task Should_Not_Lose_Lock_On_Single_Renewal_Error_And_Recover()
    {
        // Arrange: acquire succeeds, first renewal fails, second succeeds
        SetupAcquireSucceeds();

        var renewCallCount = 0;
        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                renewCallCount++;
                if (renewCallCount == 1)
                    return Task.FromException<bool>(new Exception("simulated renewal failure"));
                return Task.FromResult(true);
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act: advance to first renewal window (~4:30), then beyond to allow retry
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(10).Ticks); // give time to retry and succeed

        // Assert: lock kept, no lost notification
        service.HasLock.Should().BeTrue();
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Renewal_Failures_But_Regain()
    {
        // Arrange: acquire succeeds, renewals fail until lock is lost, then acquire again
        var acquireCallCount = 0;
        _lockProviderMock.AcquireLockAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                acquireCallCount++;
                var token = Guid.NewGuid().ToString();
                return Task.FromResult<(string Token, DateTimeOffset Expiry)?>(
                    (token, _scheduler.Now.Add(ci.Arg<TimeSpan>())));
            });

        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                // Keep failing until a Lost notification, then start succeeding
                if (!_notifications.Any(n => n.LockStatus == DistirbutedLockStatus.Lost))
                    return Task.FromResult(false); // provider says: you don't own the lock
                return Task.FromResult(true);
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to renewal window, keep going to trigger lost + reacquire
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(10).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(1).Ticks + TimeSpan.FromSeconds(20).Ticks);

        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Renewal_Failures()
    {
        // Arrange: acquire succeeds once, all renewals fail, then acquire also fails
        var acquireCount = 0;
        _lockProviderMock.AcquireLockAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                acquireCount++;
                if (acquireCount > 1)
                    return Task.FromResult<(string Token, DateTimeOffset Expiry)?>(null);
                var token = Guid.NewGuid().ToString();
                return Task.FromResult<(string Token, DateTimeOffset Expiry)?>(
                    (token, _scheduler.Now.Add(ci.Arg<TimeSpan>())));
            });

        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(false); // provider says: you don't own the lock

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to renewal window
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks);

        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task ClockDelay_Should_Tick_About_36_Times_When_Advancing_3_Minutes()
    {
        var clock = new RxSchedulerClock(_scheduler);
        var count = 0;
        var cts = new CancellationTokenSource();

        var until = _scheduler.Now + TimeSpan.FromMinutes(3);
        var looping = Task.Run(async () =>
        {
            while (_scheduler.Now < until)
            {
                await clock.Delay(TimeSpan.FromSeconds(5), cts.Token);
                count++;
            }
        });
        await Task.Delay(10);

        for (var i = 0; i < 36; i++)
        {
            _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
        }
        await looping;

        count.Should().BeInRange(35, 37);
    }

    public void Dispose()
    {
        try
        { _testCts.Cancel(); }
        catch { /* ignore */ }
        _testCts.Dispose();
        _lockProviderMock.Dispose();
    }

    [Fact]
    public async Task Should_Acquire_Lock_When_No_Existing_Lock()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().ContainSingle(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _lockProviderMock.Received(1).AcquireLockAsync(
            Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Acquire_Lock_When_Existing_Lock_Expired()
    {
        // Arrange: provider handles expiry check internally, just returns success
        SetupAcquireSucceeds();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().ContainSingle(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Not_Acquire_Lock_When_Existing_Lock_Valid()
    {
        // Arrange: provider returns null (lock held by someone else)
        _lockProviderMock.AcquireLockAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns((ValueTuple<string, DateTimeOffset>?)null);

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        Advance(TimeSpan.FromMilliseconds(500));

        // Assert
        service.HasLock.Should().BeFalse();
        _notifications.Should().BeEmpty();
    }

    [Fact]
    public async Task Should_Renew_Lock_Periodically()
    {
        // Arrange
        SetupAcquireSucceeds();
        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        service.HasLock.Should().BeTrue();

        // Advance past renewal window
        Advance(TimeSpan.FromMinutes(5));
        Advance(TimeSpan.FromSeconds(10));

        // Assert
        service.HasLock.Should().BeTrue();
        await _lockProviderMock.Received().RenewLockAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Stop_Gracefully()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act
        try
        {
            await service.StopAsync(_testCts.Token);
        }
        catch (TaskCanceledException)
        {
            // Expected when stopping the service
        }

        // Assert - release should have been called
        await _lockProviderMock.Received().ReleaseLockAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Handle_Single_IO_Exception_During_Renewal_Without_Losing_Lock()
    {
        // Arrange
        SetupAcquireSucceeds();

        var renewCallCount = 0;
        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                renewCallCount++;
                if (renewCallCount == 1)
                    return Task.FromException<bool>(new Exception("IO error"));
                return Task.FromResult(true);
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act - wait through a renewal cycle
        Advance(TimeSpan.FromMinutes(5));
        Advance(TimeSpan.FromSeconds(10));

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Lose_Lock_After_Persistent_IO_Errors_And_Expired_Lock()
    {
        // Arrange
        SetupAcquireSucceeds();

        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(false); // lock no longer ours

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act - wait for renewal to fail
        Advance(TimeSpan.FromMinutes(6));

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Reacquire_Lock_After_IO_Errors_Resolve()
    {
        // Arrange
        SetupAcquireSucceeds();

        var renewCallCount = 0;
        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                renewCallCount++;
                // Fail all renewals -> will lose lock -> service will try to re-acquire
                return Task.FromResult(false);
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act - push past renewal to lose lock, then wait for reacquisition
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(5), 240);
        service.HasLock.Should().BeFalse();

        // Reacquire should happen on next poll since AcquireLockAsync still succeeds
        await WaitUntilAdvancingAsync(() => service.HasLock, TimeSpan.FromSeconds(5), 24);

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Handle_Intermittent_IO_Errors_Without_Losing_Lock()
    {
        // Arrange
        SetupAcquireSucceeds();

        var renewCallCount = 0;
        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                renewCallCount++;
                if (renewCallCount % 4 == 0)
                    return Task.FromException<bool>(new Exception("Intermittent IO error"));
                return Task.FromResult(true);
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act - wait for multiple renewal cycles with intermittent failures
        for (var q = 0; q < 180; q++)
            Advance(TimeSpan.FromSeconds(1));

        // Assert: Should maintain lock despite intermittent failures
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Lose_Lock_When_Renewal_Returns_False()
    {
        // Arrange
        SetupAcquireSucceeds();

        _lockProviderMock.RenewLockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act - wait for renewal attempt
        await WaitUntilAdvancingAsync(
            () => _notifications.Exists(n => n.LockStatus == DistirbutedLockStatus.Lost),
            TimeSpan.FromSeconds(5), 240);

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Write_Request_File_On_Startup()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);

        // Allow generic task run to proceed
        await Task.Yield();

        // Assert
        await _lockProviderMock.Received().RequestLockAsync(
            Arg.Is<string>(s => s.EndsWith("request-lock")),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Yield_Lock_When_ShouldYield_Returns_True()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        service.HasLock.Should().BeTrue();

        // Simulate that another instance requests the lock
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Assert: Lock should be released (Yielded)
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(1), 200);

        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Delete_Request_Lock_After_Acquiring_Lock()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Assert
        service.HasLock.Should().BeTrue();
        await _lockProviderMock.Received().DeleteRequestLockAsync(
            Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Delete_Request_Lock_On_Stop_When_Not_Own()
    {
        // Arrange: Someone else has the lock
        _lockProviderMock.AcquireLockAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns((ValueTuple<string, DateTimeOffset>?)null);

        // ShouldYield returns false during stop => it's ours or empty, so delete
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var service = CreateService();
        await StartServiceAsync(service);
        service.HasLock.Should().BeFalse();

        _lockProviderMock.ClearReceivedCalls();

        // Act
        await service.StopAsync(CancellationToken.None);

        // Assert: should delete request lock on stop
        await _lockProviderMock.Received().DeleteRequestLockAsync(
            Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Not_Delete_Request_File_On_Stop_If_Not_Own()
    {
        // Arrange: ShouldYield returns true => another instance's request, don't delete
        SetupAcquireSucceeds();
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);

        var service = CreateService();
        await StartServiceAsync(service);

        // Wait for yield to happen (ShouldYield is true, so service will yield)
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(1), 200);

        _lockProviderMock.ClearReceivedCalls();

        // Act
        await service.StopAsync(CancellationToken.None);

        // Assert: should NOT delete request lock (ShouldYield = true means it's someone else's)
        await _lockProviderMock.DidNotReceive().DeleteRequestLockAsync(
            Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Should_Reclaim_Lock_After_Handover_Timeout()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        service.HasLock.Should().BeTrue();

        // Simulate yield request
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);

        // Wait for yield
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(1), 200);
        service.HasLock.Should().BeFalse();

        // Stop the yield trigger so we can reacquire
        _lockProviderMock.ShouldYieldAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(false);

        // Act: Advance time past HandoverTimeout (default 15s)
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // > 15s timeout

        // Give loop a chance
        await Task.Yield();
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
        await Task.Yield();

        // Assert
        service.HasLock.Should().BeTrue();
    }

    [Fact]
    public async Task Should_Flush_Storage_On_Stop()
    {
        // Arrange
        SetupAcquireSucceeds();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Act
        await service.StopAsync(CancellationToken.None);

        // Assert
        await _storageMock.Received().FlushAsync();
    }

    private DistributedLockService<TestEntity> CreateService()
    {
        return new DistributedLockService<TestEntity>(
            _lockProviderMock,
            Options.Create(_storageConfig),
            _loggerMock,
            _notificationsHubMock,
            _storageMock,
            Options.Create(_lockOptions),
            new RxSchedulerClock(_scheduler));
    }

    private void SetupAcquireSucceeds()
    {
        _lockProviderMock.AcquireLockAsync(Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var token = Guid.NewGuid().ToString();
                return Task.FromResult<(string Token, DateTimeOffset Expiry)?>(
                    (token, _scheduler.Now.Add(ci.Arg<TimeSpan>())));
            });
    }

    private void Advance(TimeSpan by)
    {
        _scheduler.AdvanceBy(by.Ticks);
    }

    private async Task StartServiceAsync(DistributedLockService<TestEntity> service)
    {
        await service.StartAsync(_testCts.Token);
        // Give the Task.Run loop a turn on the thread pool without sleeping
        await Task.Delay(100);
    }

    private async Task WaitForAcquiredAsync(DistributedLockService<TestEntity> service, int maxSteps = 200)
    {
        var i = 0;
        while (!service.HasLock && i++ < maxSteps)
        {
            _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
            await Task.Yield();
        }
    }

    private async Task WaitUntilAdvancingAsync(Func<bool> condition, TimeSpan step, int maxSteps)
    {
        var i = 0;
        while (!condition() && i++ < maxSteps)
        {
            _scheduler.AdvanceBy(step.Ticks);
            await Task.Yield();
        }
    }

    public class TestEntity
    {
        // Empty test entity
    }
}
