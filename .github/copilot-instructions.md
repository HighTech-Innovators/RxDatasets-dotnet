# RxDatasets-dotnet Development Guidelines

Auto-generated from all feature plans. Last updated: 2025-09-17

## Active Technologies
- C# .NET 8.0   + Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api   (002-putordeletemany-corner-cases)
- C# .NET 8.0 + Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api (55-countchildren-support)
- Multiple providers supported (Local, S3, Azure Blob, Google Cloud, Minio) (55-countchildren-support)
- C# .NET 8.0 + Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api (056-hierarchical-index)
- Protobuf files (Snapshot/Delta) on disk (Local, S3, Azure Blob, etc.) (056-hierarchical-index)

## Project Structure
```
src/
tests/
```

## Commands
# Add commands for C# .NET 8.0  

## Code Style
C# .NET 8.0  : Follow standard conventions

## Versioning and Tagging

**Tool**: `git-flow-version` - Generates semantic versioning based on git history and branch names

### Understanding git-flow-version

The project uses git-flow-version to automatically generate semantic versions. The tool produces:
- `AssemblyVersion`: Numeric format for .NET (e.g., `0.16.0.344`)
- `FullSemVer`: Complete semantic version with metadata (e.g., `0.16.0-dev.4.hierarchical-index.23+3a73832`)
- `SemVer`: Semantic version without metadata (e.g., `0.16.0-dev.4.hierarchical-index.23`)

**Key difference**: `FullSemVer` includes `+<commit-hash>` suffix; `SemVer` is the version without it.

### Branch Naming Convention

The branch name affects the pre-release label:
- **Main/Release branches**: Generate release versions (e.g., `0.16.0`)
- **Feature branches**: Generate pre-release versions with branch info (e.g., `0.16.0-dev.4.hierarchical-index.23`)
- **Pattern**: `<major>.<minor>.<patch>-dev.<number>.<branch-name>.<commits-since-tag>`

### Generating Tags and Pushing Versions

When a feature is complete and you want to create a release tag:

#### Step 1: Extract the SemVer
```powershell
$version = (git-flow-version -f 2>&1 | ConvertFrom-Json).SemVer
Write-Host "Version to tag: $version"
```

#### Step 2: Create an Unannotated Tag (Important!)
Use unannotated tags (lightweight tags) without the `+<commit-hash>` suffix:
```powershell
git tag $version
```

**Why unannotated?** 
- Lightweight tags are preferred for auto-generated versions
- Keeps the tag simple and matches the SemVer exactly
- No duplicate metadata (the version string is sufficient)

#### Step 3: Push the Tag Only to Origin
```powershell
git push origin $version
```

**Important**: Only push the tag, NOT the branch. The `origin $version` syntax pushes only the specific tag.

#### Step 4: Verify the Tag
```powershell
git describe --tags --exact-match
git tag -l $version
```

### Complete Workflow Example

```powershell
# 1. Ensure clean working directory (required!)
git status

# 2. Get the current semantic version
$version = (git-flow-version 2>&1 | ConvertFrom-Json).SemVer

# 3. Verify the version looks correct
Write-Host "Creating tag: $version"

# 4. Check if tag already exists
git tag -l $version

# 5. Create unannotated tag
git tag $version

# 6. Push tag only (not the branch)
git push origin $version

# 7. Verify
git describe --tags --exact-match
```

### When to Create Tags

- **After Phase completion**: When all tests pass and features are complete
- **Before merging to main**: Consider creating a tag for the feature branch version
- **For releases**: When merging to main/release branches (creates actual release versions)

### CI/CD Integration

The `.gitlab-ci.yml` uses `$CI_COMMIT_TAG` to detect tagged commits:
- If a commit is tagged, CI runs with `VERSION="${CI_COMMIT_TAG:-0.0.0-cibuild}"`
- Docker builds, NuGet packages, and releases are triggered by git tags
- Only tagged commits build Docker images and publish to package registries

### Troubleshooting

**Issue**: `git-flow-version` requires a clean working directory
- **Solution**: Commit all changes before tagging. Only tag with a clean working directory (no `-f` flag allowed)

**Issue**: Tag points to wrong commit
- **Solution**: Use `git tag -d $version && git push origin :$version` to delete locally and remotely, then recreate

**Issue**: Can't push tag due to pre-existing tag on different commit
- **Solution**: Delete the old tag first: `git push origin :refs/tags/$version`

### Agent Instructions for Tagging

When asked to "tag and push this" or "create a version tag", follow these steps:

1. **Ensure clean working directory** (required for tagging):
   ```powershell
   git status
   ```
   All changes must be committed before tagging.

2. **Get the semantic version** using git-flow-version:
   ```powershell
   $version = (git-flow-version 2>&1 | ConvertFrom-Json).SemVer
   ```

3. **Verify version doesn't already exist**:
   ```powershell
   git tag -l $version
   ```

4. **Create the unannotated tag** (use SemVer WITHOUT the + suffix):
   ```powershell
   git tag $version
   ```

5. **Push ONLY the tag to origin** (not the branch):
   ```powershell
   git push origin $version
   ```

6. **Verify the tag was created and pushed**:
   ```powershell
   git describe --tags --exact-match
   ```

**Critical Points**:
- ✅ Ensure working directory is CLEAN before tagging (all changes committed)
- ✅ Use unannotated tags (lightweight), not annotated
- ✅ Use `SemVer` output (without +), NOT `FullSemVer`
- ✅ Push only the tag: `git push origin $version` (NOT `git push origin`)
- ❌ Never use `-f` flag with git-flow-version
- ❌ Never create annotated tags with messages
- ❌ Never push the branch along with the tag

## Recent Changes
- 056-hierarchical-index: Added C# .NET 8.0 + Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api
- 056-hierarchical-index: Added [e.g., Python 3.11, Swift 5.9, Rust 1.75 or NEEDS CLARIFICATION] + [e.g., FastAPI, UIKit, LLVM or NEEDS CLARIFICATION]
- 55-countchildren-support: Added C# .NET 8.0 + Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
