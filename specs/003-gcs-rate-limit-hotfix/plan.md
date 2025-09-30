---
spec-version: 1
spec-id: 003-gcs-rate-limit-hotfix
title: GCS Rate Limit Hotfix - Plan
---

# Plan

This plan details the steps to implement the GCS rate limit hotfix.

## 1. Modify `GoogleCloudStorage.cs`

The core of the work is in `src/RxDatasets.StorageProvider.GoogleCloud/GoogleCloudStorage.cs`.

### 1.1. Add Rate Limit Detection Logic

A private static method `IsGcs429(Exception ex)` will be added to the class. This method will inspect the exception and its inner exceptions to determine if it's a rate limit error from Google Cloud Storage. It will check for HTTP status code 429 and specific gRPC status codes and messages.

### 1.2. Update `UploadBytes` Method

The `UploadBytes` method will be modified to include a retry loop.
- The loop will run up to 5 times.
- Inside the loop, a `try-catch` block will wrap the call to `_storageClient.UploadObjectAsync`.
- A `catch` block will specifically handle exceptions where `IsGcs429(ex)` returns `true`.
- If a rate limit error is caught, the method will log a warning and then `await Task.Delay(1100)` before the next iteration.
- If the loop completes without a successful upload, a final exception will be thrown.

## 2. Verification

- No new tests will be added for this hotfix, as it's a quick fix for an intermittent issue. Manual verification or observation in the target environment will be the primary method of validation.
