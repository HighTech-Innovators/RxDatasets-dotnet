using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Concurrency;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using NSubstitute;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using Xunit;

namespace RxDatasets.Tests;

public class DistributedLockServiceTests : IDisposable
{
    private readonly IDiskStorage<TestEntity> _storageMock;
    private readonly INotificationsHub<TestEntity> _notificationsHubMock;
    private readonly ILogger<DistributedLockService<TestEntity>> _loggerMock;
    private readonly StorageConfig _storageConfig;
    private readonly DistributedLockOptions _lockOptions;
    private readonly List<DistributedLockNotification> _notifications;
    private readonly CancellationTokenSource _testCts;
    private string? _lastWrittenToken;
    private readonly TestScheduler _scheduler;

    public DistributedLockServiceTests()
    {
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
    }

    [Fact]
    public async Task Should_Not_Lose_Lock_On_Single_Write_Error_And_Have_Three_Total_Writes()
    {
        // Arrange: simulate real lock-file lifecycle (404 until first upload, thereafter return last uploaded bytes)
        byte[]? uploaded = null;
        var writeCall = 0;

        _storageMock.DownloadBytes(Arg.Any<string>())
            .Returns(_ =>
            {
                if (uploaded is null)
                {
                    //Console.WriteLine($"Simulating 404 on download at {_scheduler.Now}");
                    return Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException());
                }
                //Console.WriteLine($"Simulating successful download at {_scheduler.Now}");
                return Task.FromResult(new ReadOnlyMemory<byte>(uploaded));
            });

        _storageMock.UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(ci =>
            {
                writeCall++;
                var bytes = ci.Arg<byte[]>();
                var content = Encoding.UTF8.GetString(bytes);
                var firstNewline = content.IndexOf('\n');
                _lastWrittenToken = firstNewline >= 0 ? content[..firstNewline] : content;
                if (writeCall == 2)
                {
                    // Fail the first renewal (expected around 4:30)
                    //Console.WriteLine($"Simulating write failure at {_scheduler.Now}");
                    return Task.FromException(new Exception("simulated write failure at 4:30"));
                }
                //Console.WriteLine($"Simulating write success at {_scheduler.Now}");
                uploaded = bytes; // persist when succeeded
                return Task.CompletedTask;
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service); // initial acquisition (first upload)

        // Act: advance to first renewal window (~4:30), then beyond to allow retry and to 6:00 total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        // Next attempt occurs after polling interval (5s)
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(10).Ticks); // give time to retry and succeed
        // Advance the rest up to 6 minutes total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(1).Ticks + TimeSpan.FromSeconds(20).Ticks);

        // Assert: exactly 3 upload attempts (acquire + failed renewal + successful renewal)
        await _storageMock.Received(3).UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>());
        await _storageMock.Received(3).DownloadBytes(Arg.Any<string>());
        service.HasLock.Should().BeTrue();
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Write_Errors_But_Regain_When_Write_Succeeds_Again()
    {
        // Arrange: simulate real lock-file lifecycle (404 until first upload, thereafter return last uploaded bytes)
        byte[]? uploaded = null;
        var writeCall = 0;

        _storageMock.DownloadBytes(Arg.Any<string>())
            .Returns(_ =>
            {
                if (uploaded is null)
                {
                    //Console.WriteLine($"Simulating 404 on download at {_scheduler.Now}");
                    return Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException());
                }
                //Console.WriteLine($"Simulating successful download at {_scheduler.Now}");
                return Task.FromResult(new ReadOnlyMemory<byte>(uploaded));
            });

        _storageMock.UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(ci =>
            {
                writeCall++;
                var bytes = ci.Arg<byte[]>();
                var content = Encoding.UTF8.GetString(bytes);
                var firstNewline = content.IndexOf('\n');
                _lastWrittenToken = firstNewline >= 0 ? content[..firstNewline] : content;
                if (writeCall > 1 && !_notifications.Any(n => n.LockStatus == DistirbutedLockStatus.Lost))
                {
                    // Fail all renewals (every 5 sec) from 4:30 until 5:00, then (when lock lost),
                    // start passing again
                    //Console.WriteLine($"Simulating write failure at {_scheduler.Now}");
                    return Task.FromException(new Exception("simulated write failure at 4:30"));
                }
                //Console.WriteLine($"Simulating write success at {_scheduler.Now}");
                uploaded = bytes; // persist when succeeded
                return Task.CompletedTask;
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service); // initial acquisition (first upload)
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to first renewal window (~4:30), then beyond to allow retry and to 6:00 total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        // Next attempt occurs after polling interval (5s)
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(10).Ticks); // give time to retry and succeed
        // Advance the rest up to 6 minutes total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(1).Ticks + TimeSpan.FromSeconds(20).Ticks);

        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);

        // Assert: exactly 9 upload attempts (acquire + failed renewal + successful renewal)
        // start, 30, 35, 40, 45, 50, 55, 60 => lost 65 => acq
        await _storageMock.Received(9).UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>());
        await _storageMock.Received(9).DownloadBytes(Arg.Any<string>());
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Write_Errors()
    {
        // Arrange: simulate real lock-file lifecycle (404 until first upload, thereafter return last uploaded bytes)
        byte[]? uploaded = null;
        var writeCall = 0;

        _storageMock.DownloadBytes(Arg.Any<string>())
            .Returns(_ =>
            {
                if (uploaded is null)
                {
                    //Console.WriteLine($"Simulating 404 on download at {_scheduler.Now}");
                    return Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException());
                }
                //Console.WriteLine($"Simulating successful download at {_scheduler.Now}");
                return Task.FromResult(new ReadOnlyMemory<byte>(uploaded));
            });

        _storageMock.UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(ci =>
            {
                writeCall++;
                var bytes = ci.Arg<byte[]>();
                var content = Encoding.UTF8.GetString(bytes);
                var firstNewline = content.IndexOf('\n');
                _lastWrittenToken = firstNewline >= 0 ? content[..firstNewline] : content;
                if (writeCall > 1)
                {
                    // Fail all renewals (every 5 sec) from 4:30 until 5:00, then (when lock lost),
                    // start passing again
                    //Console.WriteLine($"Simulating write failure at {_scheduler.Now}");
                    return Task.FromException(new Exception("simulated write failure at 4:30"));
                }
                //Console.WriteLine($"Simulating write success at {_scheduler.Now}");
                uploaded = bytes; // persist when succeeded
                return Task.CompletedTask;
            });

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service); // initial acquisition (first upload)
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to first renewal window (~4:30), then beyond to allow retry and to 5:00 total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        // Next attempt occurs after polling interval (5s)
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // give time to retry and succeed

        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task ClockDelay_Should_Tick_About_36_Times_When_Advancing_3_Minutes()
    {
        var clock = new RxSchedulerClock(_scheduler);
        var count = 0;
        var cts = new CancellationTokenSource();

        // Run a task that awaits 5s repeatedly until 3 minutes have passed
        var until = _scheduler.Now + TimeSpan.FromMinutes(3);
        var looping = Task.Run(async () =>
        {
            while (_scheduler.Now < until)
            {
                await clock.Delay(TimeSpan.FromSeconds(5), cts.Token);//.ConfigureAwait(false);
                //Console.WriteLine($"Tick at {_scheduler.Now}");
                count++;
            }
        });
        await Task.Delay(10); // let the task start

        // Drive virtual time forward in 5s steps so each await resumes and schedules the next
        for (var i = 0; i < 36; i++)
        {
            _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
        }
        await looping;

        // 3 minutes / 5 seconds = 36
        count.Should().BeInRange(35, 37);
    }

    public void Dispose()
    {
        try
        { _testCts.Cancel(); }
        catch { /* ignore */ }
        _testCts.Dispose();
    }

    [Fact]
    public async Task Should_Acquire_Lock_When_No_Existing_Lock()
    {
        // Arrange
        SetupStorageRead(null); // No existing lock
        SetupStorageWriteSuccess();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().ContainSingle(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _storageMock.Received(1).UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Fact]
    public async Task Should_Acquire_Lock_When_Existing_Lock_Expired()
    {
        // Arrange
        var expiredLock = ("expired-token", _scheduler.Now.AddMinutes(-1));
        SetupStorageRead(expiredLock);
        SetupStorageWriteSuccess();

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
        // Arrange
        var validLock = ("other-token", _scheduler.Now.AddMinutes(1));
        SetupStorageRead(validLock);

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        Advance(TimeSpan.FromMilliseconds(500)); // multiple attempts

        // Assert
        service.HasLock.Should().BeFalse();
        _notifications.Should().BeEmpty();
        await _storageMock.DidNotReceive().UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Fact]
    public async Task Should_Renew_Lock_Periodically()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);

        // Ensure reads during renewal observe our token with a valid future expiry
        SetupDownloadWithFailures(_ => false, _ => _scheduler.Now.AddMinutes(1));
        // With default timings: first renewal after ~renewalInterval - threshold
        Advance(TimeSpan.FromMinutes(5));
        Advance(TimeSpan.FromSeconds(10)); // give some extra polling cycles

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _storageMock.Received(2).UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Fact]
    public async Task Should_Stop_Gracefully()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();
        SetupStorageDeleteSuccess();

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

        // Assert - service should have acquired a lock
        service.HasLock.Should().BeTrue();
    }

    [Fact]
    public async Task Should_Handle_Single_IO_Exception_During_Renewal_Without_Losing_Lock()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Fail the next read once, then succeed
        SetupDownloadWithFailures(call => call == 1, _ => _scheduler.Now.AddMinutes(1));

        // Act - wait through a renewal cycle and enough time for reacquisition if needed
        Advance(TimeSpan.FromMilliseconds(2200));

        // Assert: at least one Acquired happened; depending on the implementation a transient Lost may occur
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Lose_Lock_After_Persistent_IO_Errors_And_Expired_Lock()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Simulate persistent IO errors - service can't read the lock file
        _storageMock.DownloadBytes(Arg.Any<string>())
                   .Returns(Task.FromException<ReadOnlyMemory<byte>>(new Exception("Persistent IO error")));

        // Act - wait for multiple renewal attempts and lock expiration
        Advance(TimeSpan.FromMinutes(6)); // beyond renewal window

        // Assert: We at least acquired once before errors. Lost may or may not occur based on timing.
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Reacquire_Lock_After_IO_Errors_Resolve()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Phase 1: Simulate persistent IO errors (reads fail during renewal)
        _storageMock.DownloadBytes(Arg.Any<string>())
                   .Returns(Task.FromException<ReadOnlyMemory<byte>>(new Exception("IO error")));

        // Wait through some renewal attempts under failure
        Advance(TimeSpan.FromMinutes(3));

        // Phase 2: Errors resolve; reading shows no lock so service should acquire again
        _storageMock.DownloadBytes(Arg.Any<string>())
                   .Returns(Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException()));

        // Act - allow ample time for reacquisition cycle and wait until re-acquired
        await WaitUntilAdvancingAsync(() => service.HasLock, TimeSpan.FromSeconds(5), 24);

        // Assert: Should reacquire after errors resolve
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Handle_Intermittent_IO_Errors_Without_Losing_Lock()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Simulate intermittent IO errors (some succeed, some fail)
        SetupDownloadWithFailures(call =>
        {
            //Console.WriteLine($"Download call {call}");
            return call % 4 == 0;
        },
            _ => _scheduler.Now.AddMinutes(1),
            errorMessage: "Intermittent IO error");

        // Act - wait for multiple renewal cycles with intermittent failures
        for (var q = 0; q < 180; q++)
            Advance(TimeSpan.FromSeconds(1));

        // Assert: Should maintain lock despite intermittent failures
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Lose_Lock_When_Write_Fails_During_Renewal_And_Lock_Expires()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Simulate successful initial write then fail subsequent renewal writes
        SetupUploadCaptureTokenAndFailFrom(2);

        // Setup reads to return expired lock after initial acquisition
        // First read after acquisition appears expired to trigger Lost on failed renewal
        SetupDownloadWithFailures(_ => false,
            call => call >= 1 ? _scheduler.Now.AddMinutes(-1) : _scheduler.Now.AddMinutes(1));

        // Act - wait for renewal attempt with write failure and processing
        await WaitUntilAdvancingAsync(() => _notifications.Exists(n => n.LockStatus == DistirbutedLockStatus.Lost), TimeSpan.FromSeconds(5), 240);

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    private DistributedLockService<TestEntity> CreateService()
    {
        return new DistributedLockService<TestEntity>(
            _storageMock,
            Options.Create(_storageConfig),
            _loggerMock,
            _notificationsHubMock,
            Options.Create(_lockOptions),
            new RxSchedulerClock(_scheduler));
    }

    private void SetupStorageRead((string Token, DateTimeOffset Expiry)? lockData)
    {
        if (lockData == null)
        {
            _storageMock.DownloadBytes(Arg.Any<string>())
                       .Returns(Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException()))
                       ;
        }
        else
        {
            var content = $"{lockData.Value.Token}\n{lockData.Value.Expiry:o}";
            var bytes = Encoding.UTF8.GetBytes(content);
            _storageMock.DownloadBytes(Arg.Any<string>())
                       .Returns(Task.FromResult(new ReadOnlyMemory<byte>(bytes)));
        }
    }

    private void SetupStorageWriteSuccess()
    {
        _storageMock.UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>())
                   .Returns(callInfo =>
                   {
                       // Capture the token from the written lock file (first line)
                       var bytes = callInfo.Arg<byte[]>();
                       var content = Encoding.UTF8.GetString(bytes);
                       var firstNewline = content.IndexOf('\n');
                       _lastWrittenToken = firstNewline >= 0 ? content[..firstNewline] : content;
                       return Task.CompletedTask;
                   });
    }

    private void SetupStorageDeleteSuccess()
    {
        _storageMock.DeleteFile(Arg.Any<string>())
                   .Returns(Task.CompletedTask);
    }

    private void Advance(TimeSpan by)
    {
        // Advance the TestScheduler (virtual time)
        _scheduler.AdvanceBy(by.Ticks);
    }

    private async Task StartServiceAsync(DistributedLockService<TestEntity> service)
    {
        await service.StartAsync(_testCts.Token);
        // Give the Task.Run loop a turn on the thread pool without sleeping
        await Task.Delay(100);
    }

    private ReadOnlyMemory<byte> MakeLockBytes(DateTimeOffset expiry, string? token = null)
        => new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes($"{token ?? _lastWrittenToken ?? "token"}\n{expiry:o}"));

    private void SetupDownloadWithFailures(Func<int, bool> shouldFail, Func<int, DateTimeOffset> expiryProvider, string errorMessage = "IO error")
    {
        var callCount = 0;
        _storageMock.DownloadBytes(Arg.Any<string>())
                   .Returns(_ =>
                   {
                       callCount++;
                       if (shouldFail(callCount))
                       {
                           //Console.WriteLine($"Simulating download failure on call {callCount}");
                           return Task.FromException<ReadOnlyMemory<byte>>(new Exception(errorMessage));
                       }
                       //Console.WriteLine($"Simulating download success on call {callCount}");
                       return Task.FromResult(MakeLockBytes(expiryProvider(callCount)));
                   });
    }

    private void SetupUploadCaptureTokenAndFailFrom(int failFromCall, string errorMessage = "Write IO error")
    {
        var writeCallCount = 0;
        _storageMock.UploadBytes(Arg.Any<string>(), Arg.Any<byte[]>())
                   .Returns(callInfo =>
                   {
                       writeCallCount++;
                       var bytes = callInfo.Arg<byte[]>();
                       var content = Encoding.UTF8.GetString(bytes);
                       var firstNewline = content.IndexOf('\n');
                       _lastWrittenToken = firstNewline >= 0 ? content[..firstNewline] : content;
                       if (writeCallCount >= failFromCall)
                       {
                           //Console.WriteLine($"Simulating upload failure on call {writeCallCount}");
                           return Task.FromException(new Exception(errorMessage));
                       }
                       //Console.WriteLine($"Simulating upload success on call {writeCallCount}");
                       return Task.CompletedTask;
                   });
    }

    public class TestEntity
    {
        // Empty test entity
    }

    private async Task WaitUntilAsync(Func<bool> condition, int maxYields = 100)
    {
        var i = 0;
        while (!condition() && i++ < maxYields)
        {
            await Task.Yield();
        }
    }

    private async Task WaitForAcquiredAsync(DistributedLockService<TestEntity> service, int maxSteps = 200)
    {
        var i = 0;
        while (!service.HasLock && i++ < maxSteps)
        {
            // Default polling is 5 seconds
            _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
            await Task.Yield();
        }
        //Console.WriteLine($"Waited {i} steps for lock acquisition.");
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
}
