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
    private readonly IConditionalDiskStorage<TestEntity> _storageMock;
    private readonly INotificationsHub<TestEntity> _notificationsHubMock;
    private readonly ILogger<DistributedLockService<TestEntity>> _loggerMock;
    private readonly StorageConfig _storageConfig;
    private readonly DistributedLockOptions _lockOptions;
    private readonly List<DistributedLockNotification> _notifications;
    private readonly CancellationTokenSource _testCts;
    private string? _lastWrittenToken;
    private readonly TestScheduler _scheduler;
    private Func<string, Task<ReadOnlyMemory<byte>>> _requestLockHandler;

    public DistributedLockServiceTests()
    {
        _storageMock = Substitute.For<IConditionalDiskStorage<TestEntity>>();
        _notificationsHubMock = Substitute.For<INotificationsHub<TestEntity>>();
        _loggerMock = Substitute.For<ILogger<DistributedLockService<TestEntity>>>();
        _storageConfig = new StorageConfig { DtoName = "TestEntity" };
        _lockOptions = new DistributedLockOptions(); // use defaults
        _notifications = new List<DistributedLockNotification>();
        _testCts = new CancellationTokenSource();
        _requestLockHandler = _ => Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException());

        _notificationsHubMock.Notify(Arg.Any<DistributedLockNotification>())
                .Returns(Task.CompletedTask)
                .AndDoes(call => _notifications.Add(call.Arg<DistributedLockNotification>()));

        _scheduler = new TestScheduler();
        // Nudge virtual time away from DateTimeOffset.MinValue to allow negative offsets safely
        _scheduler.AdvanceBy(TimeSpan.FromHours(1).Ticks);
    }

    [Fact]
    public void Should_Refuse_Storage_Without_Conditional_Writes()
    {
        var act = () => new DistributedLockService<TestEntity>(
            Substitute.For<IDiskStorage<TestEntity>>(),
            Options.Create(_storageConfig),
            _loggerMock,
            _notificationsHubMock);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public async Task Should_Not_Lose_Lock_On_Single_Write_Error_And_Have_Three_Total_Writes()
    {
        // Arrange: real lock-file lifecycle; the second write (first renewal, around 4:30) fails
        SetupLockFileLifecycle(writeCall => writeCall == 2);

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

        // Assert: exactly 3 write attempts (acquire + failed renewal + successful renewal);
        // renewals are compare-and-swap on our own version, so only the acquisition reads the lock file
        await _storageMock.Received(3).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>());
        await _storageMock.Received(1).DownloadBytesWithVersion(IsLockFile());
        service.HasLock.Should().BeTrue();
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Write_Errors_But_Regain_When_Write_Succeeds_Again()
    {
        // Arrange: all renewals fail until the lock is lost, then writes pass again
        SetupLockFileLifecycle(writeCall =>
            writeCall > 1 && !_notifications.Exists(n => n.LockStatus == DistirbutedLockStatus.Lost));

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service); // initial acquisition (first upload)
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to first renewal window (~4:30), then beyond to allow retry and to 6:00 total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        // Next attempt occurs after polling interval (5s)
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(10).Ticks);
        // Advance the rest up to 6 minutes total
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(1).Ticks + TimeSpan.FromSeconds(20).Ticks);

        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);

        // Assert: acquire, failed renewals every 5s from 4:30 until the lock expires (lost), then re-acquire
        await _storageMock.Received(9).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>());
    }

    [Fact]
    public async Task Should_Lose_Lock_On_Repeated_Write_Errors()
    {
        // Arrange: every write after the acquisition fails
        SetupLockFileLifecycle(writeCall => writeCall > 1);

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service); // initial acquisition (first upload)
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Clear();

        // Act: advance to first renewal window (~4:30), then beyond the lock expiry
        _scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // trigger first renewal attempt -> fails
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks);

        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
    }

    [Fact]
    public async Task Should_Lose_Lock_When_Renewal_Finds_Lock_Taken_Over()
    {
        // Arrange
        SetupLockFileLifecycle(_ => false);

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Another instance takes the lock: our version no longer matches and the file holds its token
        SetupStorageRead(("other-token", _scheduler.Now.AddMinutes(10)));
        _storageMock.TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>()).Returns((string?)null);

        // Act: through the renewal window
        Advance(TimeSpan.FromMinutes(5));

        // Assert
        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
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

        // Assert: created only if absent
        service.HasLock.Should().BeTrue();
        _notifications.Should().ContainSingle(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _storageMock.Received(1).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Is<string?>(v => v == null));
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

        // Assert: replaced only if still the expired version we read
        service.HasLock.Should().BeTrue();
        _notifications.Should().ContainSingle(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _storageMock.Received(1).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Is<string?>(ReadVersion));
    }

    [Fact]
    public async Task Should_Not_Acquire_Lock_When_Expired_Lock_Was_Taken_Concurrently()
    {
        // Arrange: lock looks expired, but another instance replaces it between our read and our write
        SetupStorageRead(("expired-token", _scheduler.Now.AddMinutes(-1)));
        _storageMock.TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>()).Returns((string?)null);

        var service = CreateService();

        // Act
        await StartServiceAsync(service);
        Advance(TimeSpan.FromSeconds(30));

        // Assert
        service.HasLock.Should().BeFalse();
        _notifications.Should().BeEmpty();
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
        await _storageMock.DidNotReceive().TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>());
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

        // With default timings: first renewal after ~renewalInterval - threshold
        Advance(TimeSpan.FromMinutes(5));
        Advance(TimeSpan.FromSeconds(10)); // give some extra polling cycles

        // Assert: the renewal is conditional on the version written by the acquisition
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        await _storageMock.Received(2).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>());
        await _storageMock.Received(1).TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Is<string?>("v1"));
    }

    [Fact]
    public async Task Should_Release_Lock_On_Stop_By_Expiring_It()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();
        SetupStorageDeleteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        service.HasLock.Should().BeTrue();

        // Act
        await service.StopAsync(_testCts.Token);

        // Assert: our version is overwritten with an expired lock, so the next instance can take it at once
        service.HasLock.Should().BeFalse();
        await _storageMock.Received(1).TryUploadBytes(
            IsLockFile(),
            Arg.Is<byte[]>(b =>
                Encoding.UTF8.GetString(b).EndsWith(DateTimeOffset.MinValue.ToString("o"), StringComparison.Ordinal)),
            Arg.Is<string?>("v1"));
        await _storageMock.DidNotReceive().DeleteFile(IsLockFile());
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

        // Fail the next write once, then succeed
        SetupWriteFailures(call => call == 1);

        // Act - wait through a renewal cycle and a retry
        Advance(TimeSpan.FromMinutes(5));

        // Assert
        service.HasLock.Should().BeTrue();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().NotContain(n => n.LockStatus == DistirbutedLockStatus.Lost);
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

        // Simulate persistent IO errors - service can't write the lock file
        SetupWriteFailures(_ => true, "Persistent IO error");

        // Act - wait for multiple renewal attempts and lock expiration
        Advance(TimeSpan.FromMinutes(6)); // beyond renewal window

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
        service.HasLock.Should().BeFalse();
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

        // Phase 1: Simulate persistent IO errors until the lock expires
        SetupWriteFailures(_ => true, "IO error");
        _storageMock.DownloadBytesWithVersion(IsLockFile())
                   .Returns(Task.FromException<(ReadOnlyMemory<byte>, string)>(new Exception("IO error")));
        Advance(TimeSpan.FromMinutes(6));
        service.HasLock.Should().BeFalse();

        // Phase 2: Errors resolve; reading shows no lock so service should acquire again
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

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
        SetupWriteFailures(call => call % 4 == 0, "Intermittent IO error");

        // Act - wait for multiple renewal cycles with intermittent failures
        for (var q = 0; q < 18 * 60; q++)
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

        // Successful initial write, then fail subsequent renewal writes
        SetupUploadCaptureTokenAndFailFrom(1);

        // Act - wait for renewal attempt with write failure and processing
        await WaitUntilAdvancingAsync(() => _notifications.Exists(n => n.LockStatus == DistirbutedLockStatus.Lost), TimeSpan.FromSeconds(5), 240);

        // Assert
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Acquried);
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Write_Request_File_On_Startup()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();

        // Act
        await StartServiceAsync(service);

        // Allow generic task run to proceed
        await Task.Yield();

        // Assert
        // Implementation T007 will write this file on start
        await _storageMock.Received().UploadBytes(
            Arg.Is<string>(s => s.EndsWith("request-lock")),
            Arg.Any<byte[]>()
        );
    }

    [Fact]
    public async Task Should_Yield_Lock_When_Request_File_Detected_With_Different_Token()
    {
        // Arrange
        SetupStorageRead(null); // Initially no lock
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        service.HasLock.Should().BeTrue();

        // Simulate that another instance has written a request file
        var otherToken = Guid.NewGuid().ToString();
        var requestFileContent = Encoding.UTF8.GetBytes(otherToken);

        _requestLockHandler = _ => Task.FromResult(new ReadOnlyMemory<byte>(requestFileContent));

        // Assert
        // Lock should be released (Yielded)
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(1), 200);

        service.HasLock.Should().BeFalse();
        _notifications.Should().Contain(n => n.LockStatus == DistirbutedLockStatus.Lost);
    }

    [Fact]
    public async Task Should_Delete_Request_File_After_Acquiring_Lock()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();
        SetupStorageDeleteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);

        // Assert
        service.HasLock.Should().BeTrue();
        await _storageMock.Received().DeleteFile(Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Should_Delete_Request_File_On_Stop_When_Not_Acquired()
    {
        // Arrange: Someone else has the lock
        SetupStorageRead(("other-token", DateTimeOffset.UtcNow.AddMinutes(10)));
        SetupStorageWriteSuccess();
        SetupStorageDeleteSuccess();

        // Capture what is written to request-lock so we can return it when read
        var capturedRequestBytes = Array.Empty<byte>();
        _storageMock.UploadBytes(Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)), Arg.Any<byte[]>())
            .Returns(ci => {
                capturedRequestBytes = ci.Arg<byte[]>();
                return Task.CompletedTask;
            });

        // Dynamically return captured content
        _requestLockHandler = _ => Task.FromResult(new ReadOnlyMemory<byte>(capturedRequestBytes));

        var service = CreateService();
        await StartServiceAsync(service);

        // Assert creation
        await _storageMock.Received().UploadBytes(
            Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)),
            Arg.Any<byte[]>()
        );

        service.HasLock.Should().BeFalse();

        // Clear received calls so we can check for deletion specifically during Stop
        _storageMock.ClearReceivedCalls();

        // Act
        await service.StopAsync(CancellationToken.None);

        // Assert deletion (should read file, match content, then delete)
        await _storageMock.Received().DeleteFile(Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Should_Reclaim_Lock_After_Handover_Timeout()
    {
        // Arrange
        SetupStorageRead(null);
        SetupStorageWriteSuccess();
        SetupStorageDeleteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);
        await WaitForAcquiredAsync(service);
        service.HasLock.Should().BeTrue();

        // Simulate Request
        var requestFileContent = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
        _requestLockHandler = _ => Task.FromResult(new ReadOnlyMemory<byte>(requestFileContent));

        // Wait for yield
        await WaitUntilAdvancingAsync(() => !service.HasLock, TimeSpan.FromSeconds(1), 200);
        service.HasLock.Should().BeFalse();

        // The requester never takes the (released) lock
        _storageMock.ClearReceivedCalls();

        // Act: Advance time past HandoverTimeout (default 15s), then let the next poll acquire.
        // If in Yielded state for > HandoverTimeout, the service goes back to acquiring.
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(30).Ticks); // > 15s timeout

        // Give loop a chance
        await Task.Yield();
        // Force poll wait
        _scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
        await Task.Yield();

        // Assert
        service.HasLock.Should().BeTrue();
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

    [Fact]
    public async Task Should_Not_Delete_Request_File_On_Stop_If_Not_Own()
    {
        // Arrange
        // Verify current behavior: File exists with DIFFERENT token
        var otherToken = "other-instance-id";
        _requestLockHandler = _ => Task.FromResult(new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(otherToken)));

        SetupStorageRead(null);
        SetupStorageWriteSuccess();

        var service = CreateService();
        await StartServiceAsync(service);

        // Clear calls from startup
        _storageMock.ClearReceivedCalls();

        // Act
        await service.StopAsync(CancellationToken.None);

        // Assert
        // Should NOT have deleted the request lock because the mock returns 'otherToken'
        await _storageMock.DidNotReceive().DeleteFile(Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)));
    }

    private const string ReadVersion = "v-read";

    private static string IsLockFile() => Arg.Is<string>(s => s.EndsWith("lock-file", StringComparison.Ordinal));

    private static string? TokenOf(byte[] lockBytes)
    {
        var content = Encoding.UTF8.GetString(lockBytes);
        var firstNewline = content.IndexOf('\n');
        return firstNewline >= 0 ? content[..firstNewline] : content;
    }

    /// <summary>
    /// Fixed lock-file content (null = absent) with version <see cref="ReadVersion"/>; request-lock via _requestLockHandler.
    /// </summary>
    private void SetupStorageRead((string Token, DateTimeOffset Expiry)? lockData)
    {
        // Default behavior for request-lock: return Not Found (404) so we don't accidentally yield
        _storageMock.DownloadBytes(Arg.Is<string>(s => s.EndsWith("request-lock", StringComparison.Ordinal)))
            .Returns(ci => _requestLockHandler(ci.Arg<string>()));

        if (lockData == null)
        {
            _storageMock.DownloadBytesWithVersion(IsLockFile())
                       .Returns(Task.FromException<(ReadOnlyMemory<byte>, string)>(new FileNotFoundException()));
        }
        else
        {
            var bytes = Encoding.UTF8.GetBytes($"{lockData.Value.Token}\n{lockData.Value.Expiry:o}");
            _storageMock.DownloadBytesWithVersion(IsLockFile())
                       .Returns(Task.FromResult((new ReadOnlyMemory<byte>(bytes), ReadVersion)));
        }
    }

    /// <summary>
    /// Every conditional lock write succeeds and returns a new version v1, v2, ...
    /// </summary>
    private void SetupStorageWriteSuccess() => SetupWriteFailures(_ => false);

    /// <summary>
    /// Conditional lock writes (counted from this setup, starting at 1) fail with an exception when
    /// <paramref name="shouldFail"/> says so, otherwise succeed and return a new version v1, v2, ...
    /// </summary>
    private void SetupWriteFailures(Func<int, bool> shouldFail, string errorMessage = "Write IO error")
    {
        var callCount = 0;
        var version = 0;
        _storageMock.TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>())
                   .Returns(ci =>
                   {
                       callCount++;
                       _lastWrittenToken = TokenOf(ci.Arg<byte[]>());
                       return shouldFail(callCount)
                           ? Task.FromException<string?>(new Exception(errorMessage))
                           : Task.FromResult<string?>($"v{++version}");
                   });
    }

    private void SetupUploadCaptureTokenAndFailFrom(int failFromCall, string errorMessage = "Write IO error")
        => SetupWriteFailures(call => call >= failFromCall, errorMessage);

    /// <summary>
    /// A real compare-and-swap lock file: absent until the first successful write, then the last written bytes.
    /// Writes fail with an exception when <paramref name="shouldFail"/> says so (call number starts at 1).
    /// </summary>
    private void SetupLockFileLifecycle(Func<int, bool> shouldFail)
    {
        byte[]? stored = null;
        var version = 0;
        var writeCall = 0;

        SetupStorageRead(null); // request-lock handling
        _storageMock.DownloadBytesWithVersion(IsLockFile())
            .Returns(_ => stored is null
                ? Task.FromException<(ReadOnlyMemory<byte>, string)>(new FileNotFoundException())
                : Task.FromResult((new ReadOnlyMemory<byte>(stored), $"v{version}")));

        _storageMock.TryUploadBytes(IsLockFile(), Arg.Any<byte[]>(), Arg.Any<string?>())
            .Returns(ci =>
            {
                writeCall++;
                var bytes = ci.ArgAt<byte[]>(1);
                var expected = ci.ArgAt<string?>(2);
                _lastWrittenToken = TokenOf(bytes);
                if (shouldFail(writeCall))
                {
                    return Task.FromException<string?>(new Exception($"simulated write failure at {_scheduler.Now}"));
                }

                var current = stored is null ? null : $"v{version}";
                if (expected != current)
                {
                    return Task.FromResult<string?>(null);
                }

                stored = bytes; // persist when succeeded
                return Task.FromResult<string?>($"v{++version}");
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

    public class TestEntity
    {
        // Empty test entity
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
