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
    private readonly TimeSpan _interval;
    private ulong _processed;
    private DateTime _lastLog;
    private readonly Stopwatch? _sw;
    private readonly LogLevel _level;
    private readonly bool _enabled;

    public ProgressLogger(ILogger logger, string action, ulong total, TimeSpan? interval = null, LogLevel level = LogLevel.Debug)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _action = action ?? throw new ArgumentNullException(nameof(action));
        _total = total;
        _interval = interval ?? TimeSpan.FromSeconds(10);
        _level = level;

        // Check early whether logging is enabled to avoid any expensive setup
        _enabled = _logger.IsEnabled(_level);
        _processed = 0;

        if (_enabled)
        {
            _sw = Stopwatch.StartNew();
            _lastLog = DateTime.UtcNow - _interval - TimeSpan.FromSeconds(1);
            _logger.Log(_level, "{Action} - starting: {Processed}/{Total}", _action, 0L, _total);
        }
        else
        {
            // Keep lightweight placeholders when logging is disabled
            _sw = null;
            _lastLog = DateTime.MinValue;
        }
    }

    public void Increment() => Interlocked.Increment(ref _processed);

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

        if (latestItem is null)
        {
            _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) rate={Rate:F1}/s ETA={Eta}",
                _action, _processed, _total, percent, rate, eta);
        }
        else
        {
            _logger.Log(_level, "{Action} progress: {Processed}/{Total} ({Percent:F1}%) latest={Latest} rate={Rate:F1}/s ETA={Eta}",
                _action, _processed, _total, percent, latestItem, rate, eta);
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
