namespace RxDatasets.Abstractions;

public interface IDataMigrator
{
    Task<string> MigrateDataset(string migrationName);
}
