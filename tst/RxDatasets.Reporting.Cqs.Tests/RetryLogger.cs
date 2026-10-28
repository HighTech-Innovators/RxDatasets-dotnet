using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using Xunit.Sdk;

namespace RxDatasets.Reporting.Cqs.Tests;

/// <summary>
/// The reporting plugin's logger in tests. The plugin logs a failure and retries it, so a failed assertion inside a
/// reporter callback or the storage probe would otherwise only show as a timeout, or hang a test without one. This
/// logger rethrows an assertion failure from the log call, which ends the plugin's run with the assertion's message.
/// Every other logged exception is recorded in <see cref="Exceptions"/>.
/// </summary>
internal sealed class RetryLogger<T> : ILogger<T>
{
    private readonly List<Exception> _exceptions = [];

    public IReadOnlyList<Exception> Exceptions
    {
        get
        {
            lock (_exceptions)
            {
                return [.. _exceptions];
            }
        }
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (exception == null)
        {
            return;
        }

        if (exception is XunitException)
        {
            ExceptionDispatchInfo.Capture(exception).Throw();
        }

        lock (_exceptions)
        {
            _exceptions.Add(exception);
        }
    }
}
