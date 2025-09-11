using System.Diagnostics;
using System.Globalization;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace RxDatasets;

/// <summary>
/// Helper to emit throttled progress logs with percent and ETA.
/// Tracks both item-count progress and optional byte-count progress.
/// Usage: using var p = new ProgressLogger(logger, "Applying deltas", total); p.Increment(); p.IncrementByteProgress(n); p.Report();
/// </summary>
public sealed class ProgressLogger : IDisposable
{
    private static readonly NumberFormatInfo _nfi = new NumberFormatInfo
    {
        NumberGroupSeparator = ".",
        NumberDecimalSeparator = ",",
        NumberGroupSizes = new[] { 3 }
    };

    private readonly ILogger _logger;
    private readonly string _action;
    private readonly ulong _total;
    private readonly ulong? _totalBytes;
    private readonly TimeSpan _interval;
    private ulong _processed;
    private long _processedBytes;
    private TimeSpan _lastReportElapsed;
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
            // Record the last report elapsed as the current stopwatch elapsed so the first
            // throttled Report() happens only after the configured interval (we still emit
            // a "starting" message).
            _lastReportElapsed = _sw.Elapsed;
            // Only show bytes in the starting message when a total bytes target is provided.
            if (_totalBytes.HasValue)
                _logger.Log(_level, "{Action} - starting: {ProcessedFmt}/{TotalFmt} ({ProcessedBytesFmt}/{TotalBytesFmt} bytes)", _action, Format(_processed), Format(_total), Format(0L), Format(_totalBytes.Value));
            else
                _logger.Log(_level, "{Action} - starting: {ProcessedFmt}/{TotalFmt}", _action, Format(_processed), Format(_total));
        }
        else
        {
            // Keep lightweight placeholders when logging is disabled
            _sw = null;
            _lastReportElapsed = TimeSpan.Zero;
        }
    }

    // Note: single canonical constructor is used to avoid overload ambiguity when named args are used.

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

        if ((_sw!.Elapsed - _lastReportElapsed) >= _interval)
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

        if ((_sw!.Elapsed - _lastReportElapsed) >= _interval)
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

        if ((_sw!.Elapsed - _lastReportElapsed) >= _interval)
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
        var processed = _processed;
        double percent = _total <= 0 ? 100.0 : (100.0 * processed / _total);
        double rate = elapsed.TotalSeconds <= 0 ? 0 : processed / elapsed.TotalSeconds;
        double remaining = Math.Max(0, (double)(_total > processed ? _total - processed : 0));
        var eta = rate > 0 && remaining > 0 ? TimeSpan.FromSeconds(remaining / rate) : TimeSpan.Zero;

        // bytes
        var processedBytes = Interlocked.Read(ref _processedBytes);
        double bytesRate = elapsed.TotalSeconds <= 0 ? 0 : processedBytes / elapsed.TotalSeconds;
        double? bytesPercent = null;
        TimeSpan? bytesEta = null;
        if (_totalBytes.HasValue)
        {
            bytesPercent = _totalBytes.Value <= 0 ? 100.0 : (100.0 * processedBytes / _totalBytes.Value);
            var remainingBytes = Math.Max(0, (double)(_totalBytes.Value > (ulong)processedBytes ? _totalBytes.Value - (ulong)processedBytes : 0));
            bytesEta = bytesRate > 0 && remainingBytes > 0 ? TimeSpan.FromSeconds(remainingBytes / bytesRate) : TimeSpan.Zero;
        }

        // Pre-format human readable numbers (thousands with '.')
        string processedFmt = Format(processed);
        string totalFmt = Format(_total);
        string processedBytesFmt = Format(processedBytes);
        string totalBytesFmt = _totalBytes.HasValue ? Format(_totalBytes.Value) : string.Empty;
        string rateFmt = FormatDouble(rate);
        string bytesRateFmt = FormatDouble(bytesRate);
        string percentFmt = percent.ToString("F1", _nfi);
        string bytesPercentFmt = bytesPercent.HasValue ? bytesPercent.Value.ToString("F1", _nfi) : string.Empty;

        string message;
        // Only show bytes once we've actually seen bytes reported. If no bytes were ever
        // incremented, hide the bytes section entirely (even if a total bytes target was
        // supplied) to avoid misleading 0/0 output.
        bool haveBytes = processedBytes > 0;

        if (latestItem is null)
        {
            if (haveBytes)
            {
                if (_totalBytes.HasValue)
                    message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) rate={rateFmt}/s ETA={eta} bytes: {processedBytesFmt}/{totalBytesFmt} ({bytesPercentFmt}%) rate={bytesRateFmt}B/s ETA={bytesEta}";
                else
                    message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) rate={rateFmt}/s ETA={eta} bytes: {processedBytesFmt} rate={bytesRateFmt}B/s";
            }
            else
            {
                message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) rate={rateFmt}/s ETA={eta}";
            }
        }
        else
        {
            if (haveBytes)
            {
                if (_totalBytes.HasValue)
                    message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) latest={latestItem} rate={rateFmt}/s ETA={eta} bytes: {processedBytesFmt}/{totalBytesFmt} ({bytesPercentFmt}%) rate={bytesRateFmt}B/s ETA={bytesEta}";
                else
                    message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) latest={latestItem} rate={rateFmt}/s ETA={eta} bytes: {processedBytesFmt} rate={bytesRateFmt}B/s";
            }
            else
            {
                message = $"{_action} progress: {processedFmt}/{totalFmt} ({percentFmt}%) latest={latestItem} rate={rateFmt}/s ETA={eta}";
            }
        }

        _logger.Log(_level, message);

        // Update last report to the current stopwatch elapsed.
        _lastReportElapsed = _sw?.Elapsed ?? TimeSpan.Zero;
    }

    public void Done()
    {
        if (!_enabled)
            return;

        _sw?.Stop();
        var elapsed = _sw?.Elapsed ?? TimeSpan.Zero;
        var processed = _processed;
        _logger.Log(_level, "{Action} finished: {ProcessedFmt}/{TotalFmt} elapsed={Elapsed:g}", _action, Format(processed), Format(_total), elapsed);
    }

    public void Dispose()
    {
        Done();
    }

    private static string Format(ulong value) => value.ToString("N0", _nfi);
    private static string Format(long value) => value.ToString("N0", _nfi);
    private static string FormatDouble(double value) => value.ToString("N1", _nfi);
}

