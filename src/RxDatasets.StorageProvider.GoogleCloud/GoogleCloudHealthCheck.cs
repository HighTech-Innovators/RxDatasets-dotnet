using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class GoogleCloudHealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Use a lightweight check instead of a full ListVersions scan.
            // GetLatestArtifactVersion does a single reversed point-read (RowsLimit=1)
            // which is enough to verify Bigtable connectivity.
            var rowStore = serviceProvider.GetRequiredService<IVersionedArtifactRowStore>();
            var storageConfig = serviceProvider.GetRequiredService<IOptions<StorageConfig>>();
            var prefix = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);
            await rowStore.GetLatestArtifactVersion(prefix, ObjectNameTypeEnum.Delta, cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(exception: ex);
        }
    }
}
