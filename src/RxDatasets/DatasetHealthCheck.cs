using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

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

        if (healthCheckResults.Any(r => r.Status != HealthStatus.Healthy))
        {
            return healthCheckResults.First(x => x.Status != HealthStatus.Healthy);
        }

        return HealthCheckResult.Healthy();
    }
}
public class DatasetReadinessCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var datasetReader = serviceProvider.GetRequiredService<IDatasetReader<T>>();
        var datasetWriter = serviceProvider.GetRequiredService<IDatasetWriter<T>>();

        var healthCheckResults = new List<HealthCheckResult>();

        if (datasetReader is DatasetReader<T> reader)
        {
            healthCheckResults.Add(await reader.CheckReadinessAsync(cancellationToken));
        }

        if (datasetWriter is DatasetWriter<T> writer)
        {
            healthCheckResults.Add(await writer.CheckReadinessAsync(cancellationToken));
        }

        if (healthCheckResults.Any(r => r.Status != HealthStatus.Healthy))
        {
            return healthCheckResults.First(x => x.Status != HealthStatus.Healthy);
        }

        return HealthCheckResult.Healthy();
    }
}

public class ReadOnlyDatasetHealthCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var datasetReader = serviceProvider.GetRequiredService<IDatasetReader<T>>();

        if (datasetReader is DatasetReader<T> reader)
        {
            return await reader.CheckHealthAsync(cancellationToken);
        }

        return HealthCheckResult.Healthy();
    }
}
public class ReadOnlyDatasetReadinessCheck<T>(IServiceProvider serviceProvider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var datasetReader = serviceProvider.GetRequiredService<IDatasetReader<T>>();

        if (datasetReader is DatasetReader<T> reader)
        {
            return await reader.CheckReadinessAsync(cancellationToken);
        }

        return HealthCheckResult.Healthy();
    }
}
public class DistributedLockHealthCheck<T>(DistributedLockService<T> lockService) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        return
         Task.FromResult(lockService.HasLock ? HealthCheckResult.Healthy("Ready to process requests.")
            : HealthCheckResult.Unhealthy("Not ready - Waiting for lock."));

    }
}
