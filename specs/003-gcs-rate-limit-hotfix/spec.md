---
spec-version: 1
spec-id: 003-gcs-rate-limit-hotfix
title: GCS Rate Limit Hotfix
description: Quick fix for GitLab issue #53; where a write to a GCS object can only happen once per second.
feature-id: 53
squad: backend
owner: TBD
state: draft
created-date: 2025-09-29
---

## Description

This document outlines a hotfix for a rate limiting issue encountered with Google Cloud Storage (GCS). When writing to a GCS object, specifically the `deltainfo.pb` file, multiple writes within a second can cause failures due to GCS's rate limits (one write per second per object).

The fix involves implementing a retry mechanism with a delay when a rate limit error is detected.

## Requirements

1. **Detect Rate Limit Errors**: A mechanism to specifically identify GCS rate limit exceptions (HTTP 429).
2. **Retry with Delay**: When a rate limit error is detected during an upload operation, the system should wait for a short period (e.g., 1.1 seconds) and then retry the operation.
3. **Retry Limit**: The retry mechanism should have a reasonable limit (e.g., 5 attempts) to prevent indefinite loops.
4. **Apply to Upload Method**: The retry logic must be applied to the `UploadBytes` method in `GoogleCloudStorage.cs`.
5. **Logging**: Log a warning when a rate limit is hit and a retry is attempted.

## Code for Error Detection

The following static method will be used to identify GCS rate limit exceptions:

```csharp
static bool IsGcs429(Exception ex) =>
    ex is Google.GoogleApiException g &&
        ((int)g.HttpStatusCode == 429 || g.Error?.Code == 429 ||
         g.Error?.Message?.Contains("rate limit for object mutation",
                                    StringComparison.OrdinalIgnoreCase) == true)
    || ex.InnerException is Google.GoogleApiException g2 && IsGcs429(g2)
    || ex is Grpc.Core.RpcException r &&
        (r.StatusCode is StatusCode.ResourceExhausted or StatusCode.Unavailable) &&
        (r.Status.Detail?.Contains("rate limit for object mutation",
                                   StringComparison.OrdinalIgnoreCase) == true);
```

## Implementation Details

The changes will be implemented in `src/RxDatasets.StorageProvider.GoogleCloud/GoogleCloudStorage.cs`. The `UploadBytes` method will be wrapped in a `for` loop to handle retries.

---
> **Note:** This is a quick fix. A more robust solution might involve a more sophisticated backoff strategy or queuing mechanism, but for the immediate issue, a simple delay and retry is sufficient.
