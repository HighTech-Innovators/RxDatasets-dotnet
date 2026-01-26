namespace RxDatasets.BackgroundServices;

public enum LockServiceState
{
    Idle,       // Initial state, or stopped
    Acquiring,  // Trying to get lock (Request file written)
    Active,     // Recursively renewing lock (Request file deleted)
    Yielded     // Voluntarily released lock, waiting for handover timeout
}
