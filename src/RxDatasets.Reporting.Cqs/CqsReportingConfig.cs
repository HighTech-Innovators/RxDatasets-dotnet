using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Reporting.Cqs;

public class CqsReportingConfig : IValidatableObject
{
    [Required]
    public string Endpoint { get; set; } = "";

    /// <summary>
    /// The name of this reporter's processed version (its cursor) in the dataset. Two CQS reporters on one dataset, such
    /// as an old and a new CQS during a migration, each need their own. A new name has no processed version yet, so it
    /// starts as <see cref="ReplayRetainedHistory"/> says.
    /// </summary>
    [Required]
    public string ReportingIdentifier { get; set; } = "cqs-reporter";

    /// <summary>
    /// How long CQS holds a push waiting for its batch to be persisted before it answers <c>durable = false</c>; sent with
    /// every push. The client gives up a few seconds later, so CQS always answers first.
    /// </summary>
    public TimeSpan DurabilityTimeout { get; set; } = TimeSpan.FromSeconds(35);

    /// <summary>
    /// What reporting does when it is enabled on a dataset that already has deltas (it has no processed version yet).
    /// False, the default: it starts at the newest delta and reports only the changes after it. True: it sends every
    /// delta the dataset still keeps, from the oldest one, which on a large dataset can be millions of notifications.
    /// </summary>
    public bool ReplayRetainedHistory { get; set; }

    [Range(1, int.MaxValue)]
    public int MaxDeltasPerTransaction { get; set; } = 1000;

    [Range(1, int.MaxValue)]
    public int MaxNotificationsPerPush { get; set; } = 10000;

    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (!Uri.TryCreate(Endpoint, UriKind.Absolute, out Uri? endpoint)
            || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
        {
            yield return new ValidationResult("Endpoint must be an absolute HTTP or HTTPS URI.", [nameof(Endpoint)]);
        }

        // Sent as whole milliseconds in an int32 field, where 0 would mean the server's default.
        if (DurabilityTimeout <= TimeSpan.Zero || DurabilityTimeout.TotalMilliseconds > int.MaxValue)
        {
            yield return new ValidationResult(
                $"DurabilityTimeout must be positive and at most {int.MaxValue} milliseconds.", [nameof(DurabilityTimeout)]);
        }
    }
}