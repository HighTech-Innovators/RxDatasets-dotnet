namespace RxDatasets;

public static class IndexExtensions
{
    public static void ValidateAgainstVersion(this DeltaIndex index, ulong version)
    {
        if((index.DeltaVersion != version && index.DeltaVersionLegacy != version))
            throw new InvalidOperationException($"There is data corruption in index, the requested version {version} and the received versions ({index.DeltaVersion}, legacy version {index.DeltaVersionLegacy}) neither are matching.");
    }
    
    public static void ValidateAgainstVersion(this SnapshotIndex index, ulong version)
    {
        if((index.SnapshotVersion != version && index.SnapshotVersionLegacy != version))
            throw new InvalidOperationException($"There is data corruption in index, the requested version {version} and the received versions ({index.SnapshotVersion}, legacy version {index.SnapshotVersionLegacy}) neither are matching.");

    }
}