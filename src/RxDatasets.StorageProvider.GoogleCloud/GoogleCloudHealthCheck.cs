using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class GoogleCloudHealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var storageReader = serviceProvider.GetRequiredService<IStorageReader<T>>();
            await storageReader.ListVersions();
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(exception: ex);
        }
    }
}
