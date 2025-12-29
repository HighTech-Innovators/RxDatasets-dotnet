using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using RxDatasets.Models;
using Xunit;
using Xunit.Abstractions;

namespace RxDatasets.Tests.Benchmarks;

/// <summary>
/// Benchmarks hierarchical query performance with tree-walk (optimized) vs StartsWith (baseline).
/// </summary>
public class HierarchicalQueryBenchmark
{
    private readonly ITestOutputHelper _output;

    public HierarchicalQueryBenchmark(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// Generates a hierarchical tree structure with a specified depth and breadth.
    /// </summary>
    private ImmutableIndexNode BuildHierarchy(int depth, int breadthPerLevel)
    {
        var root = ImmutableIndexNode.Empty;

        // Recursively build the hierarchy
        root = RecurseAddNodes(root, ["root"], 0, depth, breadthPerLevel);

        return root;
    }

    /// <summary>
    /// Recursively adds nodes and entities to build a hierarchical structure.
    /// </summary>
    private ImmutableIndexNode RecurseAddNodes(ImmutableIndexNode node, string[] pathParts, int currentDepth,
        int maxDepth, int breadthPerLevel)
    {
        if (currentDepth >= maxDepth)
        {
            // Add an entity at this leaf
            var leafName = pathParts[^1];
            var entity = new ImmutableSnapshotEntity(
                LastDeletedVersion: ImmutableArray<ulong>.Empty,
                LastInsertedVersion: ImmutableArray<ulong>.Empty,
                LastUpdatedVersion: 1,
                Position: 0,
                Length: 100,
                Crc32: 0);
            return node.AddEntity(leafName, entity);
        }

        // Add breadth items at this level
        var result = node;
        for (int i = 0; i < breadthPerLevel; i++)
        {
            var childName = $"node{currentDepth}_item{i}";
            var newPath = new string[pathParts.Length + 1];
            Array.Copy(pathParts, newPath, pathParts.Length);
            newPath[pathParts.Length] = childName;

            var childNode = ImmutableIndexNode.Empty;
            childNode = RecurseAddNodes(childNode, newPath, currentDepth + 1, maxDepth, breadthPerLevel);

            result = result.SetChild(childName, childNode);
        }

        return result;
    }

    /// <summary>
    /// Counts entities under a parent path using tree traversal (optimized approach).
    /// Uses alloc-free array slicing with index parameters.
    /// </summary>
    private int CountEntitiesWithTreeWalk(ImmutableIndexNode root, string parentPath)
    {
        if (string.IsNullOrEmpty(parentPath))
        {
            return CountAllEntitiesRecursive(root);
        }

        var pathParts = parentPath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var targetNode = NavigateToNode(root, pathParts, 0);
        return targetNode == null ? 0 : CountAllEntitiesRecursive(targetNode);
    }

    /// <summary>
    /// Navigates to a target node using array slicing (alloc-free).
    /// Passes the same pathParts array with an incrementing index parameter.
    /// </summary>
    private ImmutableIndexNode? NavigateToNode(ImmutableIndexNode node, string[] pathParts, int index)
    {
        if (index >= pathParts.Length)
            return node;

        var part = pathParts[index];
        if (!node.Children.TryGetValue(part, out var nextNode))
            return null;

        // Recursively navigate with index + 1 (no allocation)
        return NavigateToNode(nextNode, pathParts, index + 1);
    }

    /// <summary>
    /// Recursively counts all entities in the subtree.
    /// </summary>
    private int CountAllEntitiesRecursive(ImmutableIndexNode node)
    {
        var count = node.Entities.Count;
        foreach (var child in node.Children.Values)
        {
            count += CountAllEntitiesRecursive(child);
        }
        return count;
    }

    /// <summary>
    /// Counts entities under a parent path using StartsWith (baseline approach).
    /// Flattens the entire tree and does string comparisons.
    /// </summary>
    private int CountEntitiesWithStartsWith(ImmutableIndexNode root, string parentPath)
    {
        var allEntities = FlattenEntitiesToList(root, parentPath);
        if (string.IsNullOrEmpty(parentPath))
            return allEntities.Count;

        return allEntities.Count(name => name.StartsWith(parentPath, StringComparison.Ordinal));
    }

    /// <summary>
    /// Flattens the hierarchical tree into a list of all entity paths.
    /// </summary>
    private System.Collections.Generic.List<string> FlattenEntitiesToList(ImmutableIndexNode node, string? prefix = null)
    {
        var result = new System.Collections.Generic.List<string>();

        // Add entities at this level with their full path
        foreach (var entityName in node.Entities.Keys)
        {
            var fullPath = string.IsNullOrEmpty(prefix) ? entityName : $"{prefix}/{entityName}";
            result.Add(fullPath);
        }

        // Recursively add entities from children
        foreach (var (childName, child) in node.Children)
        {
            var childPrefix = string.IsNullOrEmpty(prefix) ? childName : $"{prefix}/{childName}";
            result.AddRange(FlattenEntitiesToList(child, childPrefix));
        }

        return result;
    }

    [Fact]
    public void Benchmark_TreeWalkVsStartsWith_Depth3_Breadth10()
    {
        const int depth = 3;
        const int breadthPerLevel = 10;
        const int queryIterations = 1000;

        var root = BuildHierarchy(depth, breadthPerLevel);
        var testPaths = new[] { "node0_item0", "node0_item0/node1_item5", "node0_item0/node1_item5/node2_item3" };

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithTreeWalk(root, path);
            }
        }
        sw.Stop();
        var treeWalkTime = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithStartsWith(root, path);
            }
        }
        sw.Stop();
        var startsWithTime = sw.ElapsedMilliseconds;

        var improvement = ((double)(startsWithTime - treeWalkTime) / startsWithTime) * 100;

        _output.WriteLine($"Depth={depth}, Breadth={breadthPerLevel}, Iterations={queryIterations}");
        _output.WriteLine($"Tree-Walk Time: {treeWalkTime}ms");
        _output.WriteLine($"StartsWith Time: {startsWithTime}ms");
        _output.WriteLine($"Improvement: {improvement:F2}%");
        _output.WriteLine($"Speedup: {startsWithTime / (double)treeWalkTime:F2}x");

        Assert.True(treeWalkTime < startsWithTime, "Tree-walk should be faster than StartsWith");
    }

    [Fact]
    public void Benchmark_TreeWalkVsStartsWith_Depth5_Breadth5()
    {
        const int depth = 5;
        const int breadthPerLevel = 5;
        const int queryIterations = 500;

        var root = BuildHierarchy(depth, breadthPerLevel);
        var testPaths = new[]
        {
            "node0_item0",
            "node0_item0/node1_item2",
            "node0_item0/node1_item2/node2_item1",
            "node0_item0/node1_item2/node2_item1/node3_item0"
        };

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithTreeWalk(root, path);
            }
        }
        sw.Stop();
        var treeWalkTime = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithStartsWith(root, path);
            }
        }
        sw.Stop();
        var startsWithTime = sw.ElapsedMilliseconds;

        var improvement = ((double)(startsWithTime - treeWalkTime) / startsWithTime) * 100;

        _output.WriteLine($"Depth={depth}, Breadth={breadthPerLevel}, Iterations={queryIterations}");
        _output.WriteLine($"Tree-Walk Time: {treeWalkTime}ms");
        _output.WriteLine($"StartsWith Time: {startsWithTime}ms");
        _output.WriteLine($"Improvement: {improvement:F2}%");
        _output.WriteLine($"Speedup: {startsWithTime / (double)treeWalkTime:F2}x");

        Assert.True(treeWalkTime < startsWithTime, "Tree-walk should be faster than StartsWith");
    }

    [Fact]
    public void Benchmark_TreeWalkVsStartsWith_Depth10_Breadth2()
    {
        const int depth = 10;
        const int breadthPerLevel = 2;
        const int queryIterations = 100;

        var root = BuildHierarchy(depth, breadthPerLevel);
        var testPaths = new[]
        {
            "node0_item0",
            "node0_item0/node1_item0",
            "node0_item0/node1_item0/node2_item1",
            "node0_item0/node1_item0/node2_item1/node3_item0"
        };

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithTreeWalk(root, path);
            }
        }
        sw.Stop();
        var treeWalkTime = sw.ElapsedMilliseconds;

        sw.Restart();
        for (int i = 0; i < queryIterations; i++)
        {
            foreach (var path in testPaths)
            {
                _ = CountEntitiesWithStartsWith(root, path);
            }
        }
        sw.Stop();
        var startsWithTime = sw.ElapsedMilliseconds;

        var improvement = ((double)(startsWithTime - treeWalkTime) / startsWithTime) * 100;

        _output.WriteLine($"Depth={depth}, Breadth={breadthPerLevel}, Iterations={queryIterations}");
        _output.WriteLine($"Tree-Walk Time: {treeWalkTime}ms");
        _output.WriteLine($"StartsWith Time: {startsWithTime}ms");
        _output.WriteLine($"Improvement: {improvement:F2}%");
        _output.WriteLine($"Speedup: {startsWithTime / (double)treeWalkTime:F2}x");

        Assert.True(treeWalkTime < startsWithTime, "Tree-walk should be faster than StartsWith");
    }
}
