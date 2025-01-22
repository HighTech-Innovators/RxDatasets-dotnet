using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class StorageConfig
{
    [RegularExpression(@"^$|^[a-zA-Z0-9\-_/]+/$", ErrorMessage = "The field must be either an empty string or be a sensible alphanumeric name and end with a '/'.")]
    public string CustomRootFolder { get; set; } = "";

    [RegularExpression(@"^$|^[a-zA-Z0-9\-_\.]+$", ErrorMessage = "The field must be either an empty string or be a sensible alphanumeric name.")]
    public string DtoName { get; set; } = "";
    
    public UploadVerificationMode UploadVerificationMode { get; set; } = UploadVerificationMode.DoNotVerify;
}

public enum UploadVerificationMode
{
    VerifyAll,
    VerifyLargeDataOnly,
    DoNotVerify
}