using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class GoogleCloudHealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var storageProvider = serviceProvider.GetRequiredService<IDiskStorage<T>>();
        if (storageProvider is not GoogleCloudStorage<T> provider)
            return HealthCheckResult.Degraded();

        var isLastResultFilled = provider?.LastResult != null;
        var isLastCheckStillValid = DateTimeOffset.Now - provider!.LastResult.ObtainedAt < TimeSpan.FromMinutes(5);

        if (isLastResultFilled && isLastCheckStillValid)
            return provider?.LastResult.Exception == null ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy(exception: provider.LastResult.Exception);

        try
        {
            await provider.ListObjects("");
        }
        catch (Exception)
        {
            //ignored
        }
        return provider.LastResult.Exception != null ? HealthCheckResult.Unhealthy() : HealthCheckResult.Healthy();
    }
}
