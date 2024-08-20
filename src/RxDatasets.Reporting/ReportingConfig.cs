using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Reporting;

public class ReportingConfig
{
    [RegularExpression(@"^$|^[a-zA-Z0-9\-_/]+/$", ErrorMessage = "The field must be either an empty string or be a sensible alphanumeric name and end with a '/'.")]
    public string Prefix { get; set; } = "";

    public bool InsertVersionToFileName; // the/prefix/created/name.12.json

    public bool InsertVersionToPrefix; // the/prefix/12/created/name.json



}