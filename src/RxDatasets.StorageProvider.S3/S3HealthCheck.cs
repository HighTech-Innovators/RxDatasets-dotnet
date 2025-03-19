using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.S3;

public class S3HealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var storageProvider = serviceProvider.GetRequiredService<IDiskStorage<T>>();
        if (storageProvider is not S3Storage<T> provider)
            return HealthCheckResult.Degraded();

        var isLastResultFilled = provider?.LastResult != null;
        var isLastCheckStillValid = DateTimeOffset.Now - provider!.LastResult.ObtainedAt < TimeSpan.FromMinutes(5);

        if (isLastResultFilled && isLastCheckStillValid)
            return provider?.LastResult.Exception == null ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy(exception: provider.LastResult.Exception);

        try
        {
            await provider?.DownloadBytes("something/notimportant", 0, 1)!;
        }
        catch (Exception)
        {
            //ignored
        }
        return provider.LastResult.Exception != null ? HealthCheckResult.Unhealthy() : HealthCheckResult.Healthy();
    }
}

