# Quickstart: PUTORDELETEMANY Corner Cases Bug Fix

**Purpose**: Verify the bug fix works correctly through hands-on testing  
**Time Required**: 15-20 minutes  
**Prerequisites**: .NET 8.0 SDK, RxDatasets solution built

## Setup

### 1. Build the Solution
```bash
cd s:\Hightech\reactive-datasets\RxDatasets-dotnet
dotnet build RxDatasets.sln
```

### 2. Navigate to Test Environment
```bash
cd testapps\Host
```

## Test Scenarios

### Scenario 1: Delete + Put Same Entity

**Before Fix**: Reports 'deleted' ❌  
**After Fix**: Reports 'nothing changed' ✅

```bash
# Start the test host
dotnet run

# In another terminal, run test client:
cd ..\TestClient
dotnet run -- --scenario "delete-put-same"
```

**Expected Output**:
```
PUT entity A => version 1
PUTORDELETEMANY [Delete(A), Put(A, same_data)] => version 2
ListChanges(1, 2): No changes reported for entity A ✅
GET entity A: Returns original data ✅
```

### Scenario 2: Delete + Put Different Entity

**Test Command**:
```bash
dotnet run -- --scenario "delete-put-different"
```

**Expected Output**:
```
PUT entity A => version 1  
PUTORDELETEMANY [Delete(A), Put(A, different_data)] => version 2
ListChanges(1, 2): Changed entity A ✅
GET entity A: Returns new data ✅
```

### Scenario 3: Multiple Puts Ending with Original

**Test Command**:
```bash
dotnet run -- --scenario "multiple-puts-original"
```

**Expected Output**:
```
PUT entity A => version 1
PUTORDELETEMANY [Put(A, v1), Put(A, v2), Put(A, original)] => version 2
ListChanges(1, 2): No changes reported for entity A ✅
GET entity A: Returns original data ✅
```

### Scenario 4: Put Then Delete

**Test Command**:
```bash
dotnet run -- --scenario "put-then-delete"
```

**Expected Output**:
```
PUT entity A => version 1
PUTORDELETEMANY [Put(A, changed), Delete(A)] => version 2
ListChanges(1, 2): Deleted entity A ✅
GET entity A: Entity not found ✅
```

### Scenario 5: Create Then Delete New Entity

**Test Command**:
```bash
dotnet run -- --scenario "create-delete-new"
```

**Expected Output**:
```
PUTORDELETEMANY [Put(B, new), Delete(B)] => version 1
ListChanges(0, 1): No changes reported for entity B ✅
GET entity B: Entity not found ✅
```

## Reader/Writer Separation Test

### Test Writer-Reader Isolation

**Purpose**: Verify readers only see committed results

```bash
# Terminal 1: Start writer host
cd testapps\Host
dotnet run --profile writer

# Terminal 2: Start reader host  
cd testapps\ReadOnlyHost
dotnet run

# Terminal 3: Run isolation test
cd testapps\TestClient
dotnet run -- --scenario "reader-isolation"
```

**Expected Behavior**:
- Reader sees no changes during batch processing
- Reader sees final net result after batch commits
- No intermediate operation states visible to reader

## Performance Verification

### Batch Operation Performance

```bash
# Run performance test with multiple operations on same entities
dotnet run -- --scenario "performance-test" --batch-size 1000 --conflicts 10
```

**Expected Results**:
- Processing time should be similar or better than before fix
- Memory usage should remain stable
- No performance regression for large batches

## Troubleshooting

### Common Issues

**Issue**: Test scenarios not found  
**Solution**: Ensure TestClient has been updated with new test scenarios

**Issue**: Hosts not starting  
**Solution**: Check port availability, ensure no other instances running

**Issue**: Reader/Writer tests failing  
**Solution**: Verify both hosts are using same storage configuration

### Debug Mode

Run with detailed logging:
```bash
dotnet run -- --scenario "delete-put-same" --verbose
```

### Log Analysis

Check logs for:
- ✅ "Net effect calculation: No change detected"
- ✅ "CRC32 comparison: {initial} == {final}"  
- ✅ "Batch operation consolidated N operations to M net changes"

## Validation Checklist

After running all scenarios:

- [ ] Scenario 1: Delete + Put Same → No change reported
- [ ] Scenario 2: Delete + Put Different → Change reported  
- [ ] Scenario 3: Multiple Puts → Returns to original state
- [ ] Scenario 4: Put + Delete → Entity deleted
- [ ] Scenario 5: Create + Delete → No net change
- [ ] Reader isolation: No intermediate states visible
- [ ] Performance: No significant regression
- [ ] Logs: Correct net effect messages

## Next Steps

If all scenarios pass:
- ✅ Bug fix verified working correctly
- ✅ Ready for integration into main codebase
- ✅ All edge cases handled properly

If any scenarios fail:
- Review implementation logic in DatasetWriter.cs
- Check CRC32 calculation consistency  
- Verify operation consolidation algorithm
- Run unit tests for detailed diagnosis

## Integration Test Execution

For full automated testing:
```bash
cd tst\RxDatasets.Tests
dotnet test --filter "Category=PutOrDeleteManyCornerCases"
```

This quickstart validates the bug fix through realistic scenarios and confirms the system behaves correctly for all identified corner cases.