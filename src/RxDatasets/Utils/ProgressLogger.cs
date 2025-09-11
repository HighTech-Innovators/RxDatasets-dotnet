using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace RxDatasets;

/// <summary>
/// Helper to emit throttled progress logs with percent and ETA.
/// Usage: using var p = new ProgressLogger(logger, "Applying deltas", total); p.Report(processed, latestVersion);
/// </summary>
public sealed class ProgressLogger : IDisposable
{
    private readonly ILogger _logger;
    private readonly string _action;
    private readonly ulong _total;
    private readonly ulong? _totalBytes;
    private readonly TimeSpan _interval;
    private ulong _processed;
    private long _processedBytes;
    private DateTime _lastLog;
    private readonly Stopwatch? _sw;
    private readonly LogLevel _level;
    private readonly bool _enabled;

    public ProgressLogger(ILogger logger, string action, ulong total, ulong? totalBytes = null, TimeSpan? interval = null, LogLevel level = LogLevel.Debug)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _action = action ?? throw new ArgumentNullException(nameof(action));
        _total = total;
        _totalBytes = totalBytes;
        _interval = interval ?? TimeSpan.FromSeconds(10);
        _level = level;

        // Check early whether logging is enabled to avoid any expensive setup
        _enabled = _logger.IsEnabled(_level);
        _processed = 0;
        _processedBytes = 0;

        if (_enabled)
        {
            _sw = Stopwatch.StartNew();
            _lastLog = DateTime.UtcNow - _interval - TimeSpan.FromSeconds(1);
            if (_totalBytes.HasValue)
                _logger.Log(_level, "{Action} - starting: {Processed}/{Total} ({ProcessedBytes}/{TotalBytes} bytes)", _action, 0L, _total, 0L, _totalBytes.Value);
            else
                _logger.Log(_level, "{Action} - starting: {Processed}/{Total} ({ProcessedBytes} bytes)", _action, 0L, _total, 0L);
        }
        else
        {
            // Keep lightweight placeholders when logging is disabled
            _sw = null;
            _lastLog = DateTime.MinValue;
        }
    }

    // Compatibility overload: previous signature was (ILogger, string, ulong total, TimeSpan? interval = null, LogLevel level = LogLevel.Debug)
    public ProgressLogger(ILogger logger, string action, ulong total, TimeSpan interval, LogLevel level = LogLevel.Debug)
        : this(logger, action, total, null, interval, level)
    {
    }

    public void Increment() => Interlocked.Increment(ref _processed);

    /// <summary>
    /// Increment processed bytes. Use this from a hot loop to aggregate byte progress.
    /// </summary>
    public void IncrementByteProgress(long bytes) => Interlocked.Add(ref _processedBytes, bytes);

    public void Report(ulong processed, object? latestItem = null)
    {
        Interlocked.Exchange(ref _processed, processed);
        if (!_enabled)
            return;

        if ((DateTime.UtcNow - _lastLog) >= _interval)
        {
            Log(latestItem);
        }
    }

    /// <summary>
    /// Report using the internal processed counter (safer when caller used Increment()).
    /// </summary>
    public void Report()
    {
        if (!_enabled)
            return;

        if ((DateTime.UtcNow - _lastLog) >= _interval)
        {
            Log(null);
        }
    }

    /// <summary>
    /// Report using the internal processed counter and attach the latest item.
    /// </summary>
    public void Report(object? latestItem)
    {
        if (!_enabled)
            return;

        if ((DateTime.UtcNow - _lastLog) >= _interval)
        {
            Log(latestItem);
        }
    }

    private void Log(object? latestItem = null)
    {
        if (!_enabled)
            return;

        // Only compute heavy values when logging is enabled
        var elapsed = _sw?.Elapsed ?? TimeSpan.Zero;
        double percent = _total <= 0 ? 100.0 : (100.0 * _processed / _total);
        double rate = elapsed.TotalSeconds <= 0 ? 0 : _processed / elapsed.TotalSeconds;
        double remaining = Math.Max(0, _total - _processed);
        var eta = rate > 0 && remaining > 0 ? TimeSpan.FromSeconds(remaining / rate) : TimeSpan.Zero;

        // bytes
        var processedBytes = Interlocked.Read(ref _processedBytes);
        double bytesRate = elapsed.TotalSeconds <= 0 ? 0 : processedBytes / elapsed.TotalSeconds;
        double? bytesPercent = null;
        TimeSpan? bytesEta = null;
        if (_totalBytes.HasValue)
        {
            bytesPercent = _totalBytes.Value <= 0 ? 100.0 : (100.0 * processedBytes / _totalBytes.Value);
            var remainingBytes = Math.Max(0, (double)(_totalBytes.Value - (ulong)processedBytes));
            bytesEta = bytesRate > 0 && remainingBytes > 0 ? TimeSpan.FromSeconds(remainingBytes / bytesRate) : TimeSpan.Zero;
        }

        if (latestItem is null)
        {
            if (bytesPercent.HasValue)
            {
                var totalBytesLocal = _totalBytes!.Value;
                var bytesPercentLocal = bytesPercent!.Value;
                var bytesEtaLocal = bytesEta;
                _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) rate={Rate:F1}/s ETA={Eta} bytes: {ProcessedBytes}/{TotalBytes} ({BytesPercent:F1}%) rate={BytesRate:F1}B/s ETA={BytesEta}",
                    _action, _processed, _total, percent, rate, eta, processedBytes, totalBytesLocal, bytesPercentLocal, bytesRate, bytesEtaLocal);
            }
            else
            {
                _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) rate={Rate:F1}/s ETA={Eta} bytes: {ProcessedBytes} rate={BytesRate:F1}B/s",
                    _action, _processed, _total, percent, rate, eta, processedBytes, bytesRate);
            }
        }
        else
        {
            if (bytesPercent.HasValue)
            {
                var totalBytesLocal = _totalBytes!.Value;
                var bytesPercentLocal = bytesPercent!.Value;
                var bytesEtaLocal = bytesEta;
                _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) latest={Latest} rate={Rate:F1}/s ETA={Eta} bytes: {ProcessedBytes}/{TotalBytes} ({BytesPercent:F1}%) rate={BytesRate:F1}B/s ETA={BytesEta}",
                    _action, _processed, _total, percent, latestItem, rate, eta, processedBytes, totalBytesLocal, bytesPercentLocal, bytesRate, bytesEtaLocal);
            }
            else
            {
                _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) latest={Latest} rate={Rate:F1}/s ETA={Eta} bytes: {ProcessedBytes} rate={BytesRate:F1}B/s",
                    _action, _processed, _total, percent, latestItem, rate, eta, processedBytes, bytesRate);
            }
        }

        _lastLog = DateTime.UtcNow;
    }

    public void Done()
    {
        if (!_enabled)
            return;

        _sw?.Stop();
        var elapsed = _sw?.Elapsed ?? TimeSpan.Zero;
        _logger.Log(_level, "{Action} finished: {Processed}/{Total} elapsed={Elapsed:g}", _action, _processed, _total, elapsed);
    }

    public void Dispose()
    {
        Done();
    }
}

/// <summary>
/// Tracks byte-level progress and rate. totalBytes is optional; if absent percent/ETA are omitted.
/// Usage: using var b = new BytesProgressLogger(logger, "Copying bytes", totalBytesOptional); b.AddBytes(n); b.Report();
/// </summary>
public sealed class BytesProgressLogger : IDisposable
{
    private readonly ILogger _logger;
    private readonly string _action;
    private readonly ulong? _totalBytes;
    private readonly TimeSpan _interval;
    private long _processedBytes;
    private DateTime _lastLog;
    private readonly Stopwatch? _sw;
    private readonly LogLevel _level;
    private readonly bool _enabled;

    public BytesProgressLogger(ILogger logger, string action, ulong? totalBytes = null, TimeSpan? interval = null, LogLevel level = LogLevel.Debug)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _action = action ?? throw new ArgumentNullException(nameof(action));
        _totalBytes = totalBytes;
        _interval = interval ?? TimeSpan.FromSeconds(10);
        _level = level;

        _enabled = _logger.IsEnabled(_level);
        _processedBytes = 0;

        if (_enabled)
        {
            _sw = Stopwatch.StartNew();
            _lastLog = DateTime.UtcNow - _interval - TimeSpan.FromSeconds(1);
            if (_totalBytes.HasValue)
                _logger.Log(_level, "{Action} - starting: {ProcessedBytes}/{TotalBytes} bytes", _action, 0L, _totalBytes.Value);
            else
                _logger.Log(_level, "{Action} - starting: {ProcessedBytes} bytes", _action, 0L);
        }
        else
        {
            _sw = null;
            _lastLog = DateTime.MinValue;
        }
    }

    public void AddBytes(long bytes) => Interlocked.Add(ref _processedBytes, bytes);

    public void Report()
    {
        if (!_enabled)
            return;

        if ((DateTime.UtcNow - _lastLog) >= _interval)
        {
            Log();
        }
    }

    private void Log()
    {
        if (!_enabled)
            return;

        var elapsed = _sw?.Elapsed ?? TimeSpan.Zero;
        var processed = Interlocked.Read(ref _processedBytes);
        double rate = elapsed.TotalSeconds <= 0 ? 0 : processed / elapsed.TotalSeconds;

        if (_totalBytes.HasValue)
        {
            double percent = _totalBytes.Value <= 0 ? 100.0 : (100.0 * processed / _totalBytes.Value);
            double remaining = Math.Max(0, (double)(_totalBytes.Value - (ulong)processed));
            var eta = rate > 0 && remaining > 0 ? TimeSpan.FromSeconds(remaining / rate) : TimeSpan.Zero;
            _logger.Log(_level, "{Action} bytes: {ProcessedBytes}/{TotalBytes} ({Percent:F1}%) rate={Rate:F1}B/s ETA={Eta}",
                _action, processed, _totalBytes.Value, percent, rate, eta);
        }
        else
        {
            _logger.Log(_level, "{Action} bytes: {ProcessedBytes} rate={Rate:F1}B/s",
                _action, processed, rate);
        }

        _lastLog = DateTime.UtcNow;
    }

    public void Done()
    {
        if (!_enabled)
            return;

        _sw?.Stop();
        var elapsed = _sw?.Elapsed ?? TimeSpan.Zero;
        var processed = Interlocked.Read(ref _processedBytes);
        if (_totalBytes.HasValue)
            _logger.Log(_level, "{Action} finished: {ProcessedBytes}/{TotalBytes} bytes elapsed={Elapsed:g}", _action, processed, _totalBytes.Value, elapsed);
        else
            _logger.Log(_level, "{Action} finished: {ProcessedBytes} bytes elapsed={Elapsed:g}", _action, processed, elapsed);
    }

    public void Dispose()
    {
        Done();
    }
}
