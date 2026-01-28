using Grpc.Core;

namespace RxDatasets;

/// <summary>
/// Utility class for detecting HTTP headers from gRPC request contexts.
/// </summary>
public static class HeaderDetection
{
    /// <summary>
    /// Extracts the X-Debug header from the request context.
    /// Header matching is case-insensitive.
    /// </summary>
    /// <param name="context">The gRPC ServerCallContext containing request metadata</param>
    /// <returns>True if the X-Debug header is present, false otherwise</returns>
    public static bool HasXDebugHeader(ServerCallContext context)
    {
        if (context?.RequestHeaders == null)
            return false;

        // gRPC headers are case-insensitive, but we explicitly check common variations
        foreach (var header in context.RequestHeaders)
        {
            if (header.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
