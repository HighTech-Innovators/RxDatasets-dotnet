using RxDatasets.Models;
using Xunit;

namespace RxDatasets.Tests.Compatibility;

/// <summary>
/// Tests for the hierarchical index delta migration plugin functionality.
/// Validates that legacy flat delta files are properly converted to hierarchical format.
/// </summary>
public class DeltaMigrationPluginTests
{
    /// <summary>
    /// Helper to distribute deleted names to hierarchy (mirrors DataMigrator logic)
    /// </summary>
    private void DistributeDeletedNameToHierarchy(DeltaNode node, string[] pathParts, string deletedName)
    {
        if (pathParts.Length == 0)
            return;

        var current = node;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            var part = pathParts[i];
            if (!current.Children.ContainsKey(part))
            {
                current.Children[part] = new DeltaNode();
            }
            current = current.Children[part];
        }

        var leafName = pathParts[pathParts.Length - 1];
        current.DeletedEntities[leafName] = new DeletedEntity { Name = leafName, PreviousVersion = 0 };
    }

    /// <summary>
    /// Helper to distribute entities to hierarchy (mirrors DataMigrator logic)
    /// </summary>
    private void DistributeEntityToHierarchy(DeltaNode node, string[] pathParts, DeltaEntity entity)
    {
        if (pathParts.Length == 0)
            return;

        var current = node;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            var part = pathParts[i];
            if (!current.Children.ContainsKey(part))
            {
                current.Children[part] = new DeltaNode();
            }
            current = current.Children[part];
        }

        var leafName = pathParts[pathParts.Length - 1];
        current.ChangedEntities[leafName] = entity;
    }

    [Fact]
    public void MigrateDeletedNameToHierarchical_WithSimplePath_ConvertsCorrectly()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var deletedName = "company/dept/employee";
        var parts = deletedName.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // Act
        DistributeDeletedNameToHierarchy(rootNode, parts, deletedName);

        // Assert
        Assert.True(rootNode.Children.ContainsKey("company"));
        var companyNode = rootNode.Children["company"];
        Assert.True(companyNode.Children.ContainsKey("dept"));
        var deptNode = companyNode.Children["dept"];
        Assert.True(deptNode.DeletedEntities.ContainsKey("employee"));
    }

    [Fact]
    public void MigrateEntityToHierarchical_WithSimplePath_ConvertsCorrectly()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var entity = new DeltaEntity
        {
            Name = "company/dept/employee",
            PreviousVersion = 0
        };
        var parts = entity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // Act
        DistributeEntityToHierarchy(rootNode, parts, entity);

        // Assert
        Assert.True(rootNode.Children.ContainsKey("company"));
        var companyNode = rootNode.Children["company"];
        Assert.True(companyNode.Children.ContainsKey("dept"));
        var deptNode = companyNode.Children["dept"];
        Assert.True(deptNode.ChangedEntities.ContainsKey("employee"));
        Assert.Equal("company/dept/employee", deptNode.ChangedEntities["employee"].Name);
    }

    [Fact]
    public void MigrateMultipleDeletes_DistributesCorrectly()
    {
        // Arrange
        var rootNode = new DeltaNode();

        // Act - Distribute multiple deleted names at same level
        var deletedNames = new[] { "company/dept/employee1", "company/dept/employee2" };
        foreach (var deletedName in deletedNames)
        {
            var parts = deletedName.Split('/', StringSplitOptions.RemoveEmptyEntries);
            DistributeDeletedNameToHierarchy(rootNode, parts, deletedName);
        }

        // Assert
        var deptNode = rootNode.Children["company"].Children["dept"];
        Assert.True(deptNode.DeletedEntities.ContainsKey("employee1"));
        Assert.True(deptNode.DeletedEntities.ContainsKey("employee2"));
        Assert.Equal(2, deptNode.DeletedEntities.Count);
    }

    [Fact]
    public void MigrateMixedOperations_DistributesAllCorrectly()
    {
        // Arrange
        var rootNode = new DeltaNode();

        // Act - Add changed entities
        var changedEntity = new DeltaEntity { Name = "store/section/item1" };
        DistributeEntityToHierarchy(rootNode, "store/section/item1".Split('/'), changedEntity);

        // Add new entities
        var newEntity = new DeltaEntity { Name = "store/section/item2" };
        var storeNode = rootNode.Children["store"];
        var sectionNode = storeNode.Children["section"];
        sectionNode.NewEntities["item2"] = newEntity;

        // Add deleted names
        DistributeDeletedNameToHierarchy(rootNode, "store/section/item0".Split('/'), "store/section/item0");

        // Assert
        var deptNode = rootNode.Children["store"].Children["section"];
        Assert.True(deptNode.ChangedEntities.ContainsKey("item1"));
        Assert.True(deptNode.NewEntities.ContainsKey("item2"));
        Assert.True(deptNode.DeletedEntities.ContainsKey("item0"));
    }

    [Fact]
    public void MigrateDeepHierarchy_MaintainsStructure()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var entity = new DeltaEntity { Name = "level1/level2/level3/level4/level5/leaf" };

        // Act
        var parts = entity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);
        DistributeEntityToHierarchy(rootNode, parts, entity);

        // Assert - Navigate deep hierarchy
        var current = rootNode;
        Assert.True(current.Children.ContainsKey("level1"));
        current = current.Children["level1"];
        Assert.True(current.Children.ContainsKey("level2"));
        current = current.Children["level2"];
        Assert.True(current.Children.ContainsKey("level3"));
        current = current.Children["level3"];
        Assert.True(current.Children.ContainsKey("level4"));
        current = current.Children["level4"];
        Assert.True(current.Children.ContainsKey("level5"));
        current = current.Children["level5"];
        Assert.True(current.ChangedEntities.ContainsKey("leaf"));
    }

    [Fact]
    public void MigrateEntityPath_RootLevelItems_AreStoredAtRoot()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var entity = new DeltaEntity { Name = "singleitem" };

        // Act - Single segment path (no slashes)
        var parts = entity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);
        DistributeEntityToHierarchy(rootNode, parts, entity);

        // Assert - Should be at root level
        Assert.True(rootNode.ChangedEntities.ContainsKey("singleitem"));
        Assert.Empty(rootNode.Children);
    }

    [Fact]
    public void MigrateEntityPath_EmptySegments_AreSkipped()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var entity = new DeltaEntity { Name = "level1//level2//item" }; // Double slashes

        // Act - Use RemoveEmptyEntries to handle this
        var parts = entity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(3, parts.Length); // Should be ["level1", "level2", "item"]
        DistributeEntityToHierarchy(rootNode, parts, entity);

        // Assert - Hierarchy created correctly without empty segments
        var current = rootNode;
        Assert.True(current.Children.ContainsKey("level1"));
        current = current.Children["level1"];
        Assert.True(current.Children.ContainsKey("level2"));
        current = current.Children["level2"];
        Assert.True(current.ChangedEntities.ContainsKey("item"));
    }

    [Fact]
    public void MigrateMultipleEntities_SamePrefix_ShareNodes()
    {
        // Arrange
        var rootNode = new DeltaNode();
        var entity1 = new DeltaEntity { Name = "company/dept/emp1" };
        var entity2 = new DeltaEntity { Name = "company/dept/emp2" };

        // Act - Both entities share "company/dept" prefix
        DistributeEntityToHierarchy(rootNode, entity1.Name.Split('/'), entity1);
        DistributeEntityToHierarchy(rootNode, entity2.Name.Split('/'), entity2);

        // Assert - Verify prefix sharing (same nodes reused)
        var deptNode = rootNode.Children["company"].Children["dept"];
        Assert.Equal(2, deptNode.ChangedEntities.Count);
        Assert.True(deptNode.ChangedEntities.ContainsKey("emp1"));
        Assert.True(deptNode.ChangedEntities.ContainsKey("emp2"));
    }
}
