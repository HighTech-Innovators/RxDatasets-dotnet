using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RxDatasets.Abstractions;

namespace RxDatasets;

public class DatasetHealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var datasetReader = serviceProvider.GetRequiredService<IDatasetReader<T>>();
        var datasetWriter = serviceProvider.GetRequiredService<IDatasetWriter<T>>();

        var healthCheckResults = new List<HealthCheckResult>();

        if (datasetReader is DatasetReader<T> reader)
        {
            healthCheckResults.Add(await reader.CheckHealthAsync(cancellationToken));
        }

        if (datasetWriter is DatasetWriter<T> writer)
        {
            healthCheckResults.Add(await writer.CheckHealthAsync(cancellationToken));
        }

        if(healthCheckResults.Any(r => r.Status != HealthStatus.Healthy))
        {
            return healthCheckResults.First(x => x.Status != HealthStatus.Healthy);
        }

        return HealthCheckResult.Healthy();
    }
}
