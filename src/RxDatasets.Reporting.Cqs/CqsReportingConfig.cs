using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Reporting.Cqs;

public class CqsReportingConfig : IValidatableObject
{
    [Required]
    public string Endpoint { get; set; } = "";

    public TimeSpan DurabilityTimeout { get; set; } = TimeSpan.FromSeconds(35);

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

        if (DurabilityTimeout <= TimeSpan.Zero || DurabilityTimeout > DateTime.MaxValue - DateTime.UtcNow)
        {
            yield return new ValidationResult("DurabilityTimeout must be a positive, finite RPC deadline.",
                [nameof(DurabilityTimeout)]);
        }
    }
}