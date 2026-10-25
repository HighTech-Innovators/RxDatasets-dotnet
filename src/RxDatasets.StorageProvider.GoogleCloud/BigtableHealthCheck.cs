using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RxDatasets.StorageProvider.GoogleCloud;

/// <summary>Healthy when one row key of the Bigtable table can be read.</summary>
public class BigtableHealthCheck(BigtableRowStore rowStore) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await rowStore.Ping();
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Bigtable cannot be read", ex);
        }
    }
}
