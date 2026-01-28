using Grpc.Core;

namespace RxDatasets.Tests;

/// <summary>
/// Unit tests for Spec 067: WriteResponse Change Details
/// 
/// Tests for:
/// - T024: ChangeType enum values
/// - T025: DetailedChange message structure
/// - T026: WriteResponse backward compatibility
/// - T027: Header detection utility
/// </summary>
public class WriteResponseChangeDetailsTests
{
    #region T024: ChangeType Enum Tests

    [Fact]
    public void ChangeType_EnumValues_MatchSpecification()
    {
        // T024: Verify ChangeType enum values match proto specification
        
        // NEW should be 0
        ((int)Hightechinnovators.Dtos.Type.ChangeType.New).Should().Be(0);
        
        // CHANGED should be 1
        ((int)Hightechinnovators.Dtos.Type.ChangeType.Changed).Should().Be(1);
        
        // DELETED should be 2
        ((int)Hightechinnovators.Dtos.Type.ChangeType.Deleted).Should().Be(2);
    }

    [Fact]
    public void ChangeType_EnumNames_AreCorrect()
    {
        // Verify enum names for clarity
        Hightechinnovators.Dtos.Type.ChangeType.New.ToString().Should().Be("New");
        Hightechinnovators.Dtos.Type.ChangeType.Changed.ToString().Should().Be("Changed");
        Hightechinnovators.Dtos.Type.ChangeType.Deleted.ToString().Should().Be("Deleted");
    }

    [Fact]
    public void ChangeType_CanCompareValues()
    {
        var new1 = Hightechinnovators.Dtos.Type.ChangeType.New;
        var new2 = Hightechinnovators.Dtos.Type.ChangeType.New;
        var changed = Hightechinnovators.Dtos.Type.ChangeType.Changed;

        new1.Should().Be(new2);
        new1.Should().NotBe(changed);
    }

    #endregion

    #region T025: DetailedChange Message Structure Tests

    [Fact]
    public void DetailedChange_CanBeCreated()
    {
        // T025: Test DetailedChange message creation
        var change = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-123",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.New
        };

        change.EntityId.Should().Be("entity-123");
        change.ChangeType.Should().Be(Hightechinnovators.Dtos.Type.ChangeType.New);
    }

    [Fact]
    public void DetailedChange_CanStoreAllChangeTypes()
    {
        // T025: Test DetailedChange can hold all three change types
        var newChange = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-new",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.New
        };

        var changedChange = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-changed",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Changed
        };

        var deletedChange = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-deleted",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Deleted
        };

        newChange.ChangeType.Should().Be(Hightechinnovators.Dtos.Type.ChangeType.New);
        changedChange.ChangeType.Should().Be(Hightechinnovators.Dtos.Type.ChangeType.Changed);
        deletedChange.ChangeType.Should().Be(Hightechinnovators.Dtos.Type.ChangeType.Deleted);
    }

    [Fact]
    public void DetailedChange_CanHaveEmptyEntityId()
    {
        // T025: Edge case - empty entity ID
        var change = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.New
        };

        change.EntityId.Should().Be("");
        change.ChangeType.Should().Be(Hightechinnovators.Dtos.Type.ChangeType.New);
    }

    [Fact]
    public void DetailedChange_CanHaveLongEntityId()
    {
        // T025: Edge case - very long entity ID (1000 chars)
        var longId = new string('x', 1000);
        var change = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = longId,
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Deleted
        };

        change.EntityId.Length.Should().Be(1000);
        change.EntityId.Should().Be(longId);
    }

    [Fact]
    public void DetailedChange_CanBeCloned()
    {
        // T025: Test DetailedChange can be copied
        var original = new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-to-copy",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Changed
        };

        var copy = new Hightechinnovators.Dtos.Type.DetailedChange(original);

        copy.EntityId.Should().Be(original.EntityId);
        copy.ChangeType.Should().Be(original.ChangeType);
    }

    #endregion

    #region T026: WriteResponse Backward Compatibility Tests

    [Fact]
    public void WriteResponse_HasDetailedChangesField()
    {
        // T026: Verify WriteResponse has detailed_changes field
        var response = new Hightechinnovators.Dtos.Type.WriteResponse
        {
            NewDatasetVersion = 1,
            NumAddedEntities = 10,
            NumUpdatedEntities = 5,
            NumDeletedEntities = 2
        };

        response.DetailedChanges.Should().NotBeNull();
        response.DetailedChanges.Count.Should().Be(0); // Initially empty
    }

    [Fact]
    public void WriteResponse_CanPopulateDetailedChanges()
    {
        // T026: Verify detailed_changes can be populated
        var response = new Hightechinnovators.Dtos.Type.WriteResponse
        {
            NewDatasetVersion = 1,
            NumAddedEntities = 2,
            NumUpdatedEntities = 1,
            NumDeletedEntities = 1
        };

        response.DetailedChanges.Add(new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "new-1",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.New
        });

        response.DetailedChanges.Count.Should().Be(1);
        response.DetailedChanges[0].EntityId.Should().Be("new-1");
    }

    [Fact]
    public void WriteResponse_CanHoldMultipleDetailedChanges()
    {
        // T026: Verify WriteResponse can hold multiple detailed changes
        var response = new Hightechinnovators.Dtos.Type.WriteResponse
        {
            NewDatasetVersion = 1,
            NumAddedEntities = 1,
            NumUpdatedEntities = 1,
            NumDeletedEntities = 1
        };

        response.DetailedChanges.Add(new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-1",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.New
        });

        response.DetailedChanges.Add(new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-2",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Changed
        });

        response.DetailedChanges.Add(new Hightechinnovators.Dtos.Type.DetailedChange
        {
            EntityId = "entity-3",
            ChangeType = Hightechinnovators.Dtos.Type.ChangeType.Deleted
        });

        response.DetailedChanges.Count.Should().Be(3);
        response.DetailedChanges[0].EntityId.Should().Be("entity-1");
        response.DetailedChanges[1].EntityId.Should().Be("entity-2");
        response.DetailedChanges[2].EntityId.Should().Be("entity-3");
    }

    [Fact]
    public void WriteResponse_BackwardCompatible_EmptyDetailedChanges()
    {
        // T026: Verify field 5 is optional (backward compatible)
        // Old clients should still work with WriteResponse without detailed_changes
        var response = new Hightechinnovators.Dtos.Type.WriteResponse
        {
            NewDatasetVersion = 1,
            NumAddedEntities = 1,
            NumUpdatedEntities = 0,
            NumDeletedEntities = 0
            // field 5 (detailed_changes) not set - should be empty
        };

        response.DetailedChanges.Should().NotBeNull();
        response.DetailedChanges.Count.Should().Be(0);
    }

    [Fact]
    public void WriteResponse_HasAllRequiredFields()
    {
        // T026: Verify WriteResponse has all required fields for backward compatibility
        var response = new Hightechinnovators.Dtos.Type.WriteResponse
        {
            NewDatasetVersion = 5,
            NumAddedEntities = 10,
            NumUpdatedEntities = 5,
            NumDeletedEntities = 2
        };

        response.NewDatasetVersion.Should().Be(5);
        response.NumAddedEntities.Should().Be(10);
        response.NumUpdatedEntities.Should().Be(5);
        response.NumDeletedEntities.Should().Be(2);
    }

    #endregion

    #region T027: Header Detection Tests

    [Fact]
    public void HeaderDetection_DetectsXDebugHeader_UpperCase()
    {
        // T027: Test header detection with uppercase X-Debug
        var headerEntry = new Metadata.Entry("X-Debug", "true");
        var headers = new Metadata { headerEntry };
        
        var header = headers.FirstOrDefault(h => h.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase));
        header.Should().NotBeNull();
        // Note: gRPC Metadata converts header names to lowercase
        header.Key.Should().Be("x-debug");
    }

    [Fact]
    public void HeaderDetection_DetectsXDebugHeader_LowerCase()
    {
        // T027: Test header detection with lowercase x-debug
        var headerEntry = new Metadata.Entry("x-debug", "true");
        var headers = new Metadata { headerEntry };
        
        var header = headers.FirstOrDefault(h => h.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase));
        header.Should().NotBeNull();
        header.Key.Should().Be("x-debug");
    }

    [Fact]
    public void HeaderDetection_DetectsXDebugHeader_MixedCase()
    {
        // T027: Test header detection with mixed case X-Debug variants
        var testCases = new[] { "X-Debug", "x-debug", "X-DEBUG", "x-Debug", "X-debug" };

        foreach (var headerName in testCases)
        {
            var headerEntry = new Metadata.Entry(headerName, "true");
            var headers = new Metadata { headerEntry };
            
            var header = headers.FirstOrDefault(h => h.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase));
            header.Should().NotBeNull($"Header '{headerName}' should be detected");
        }
    }

    [Fact]
    public void HeaderDetection_IgnoresNonDebugHeaders()
    {
        // T027: Verify non-debug headers are not matched
        var headerEntry = new Metadata.Entry("X-Request-ID", "123");
        var headers = new Metadata { headerEntry };
        
        var header = headers.FirstOrDefault(h => h.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase));
        header.Should().BeNull();
    }

    [Fact]
    public void HeaderDetection_HeaderValueDoesNotMatter()
    {
        // T027: Verify any value for X-Debug header triggers detection
        var testValues = new[] { "true", "false", "1", "0", "yes", "no", "" };

        foreach (var value in testValues)
        {
            var headerEntry = new Metadata.Entry("x-debug", value);
            var headers = new Metadata { headerEntry };
            
            var header = headers.FirstOrDefault(h => h.Key.Equals("x-debug", StringComparison.OrdinalIgnoreCase));
            header.Should().NotBeNull($"Header with value '{value}' should be detected");
        }
    }

    [Fact]
    public void HeaderDetection_CaseInsensitiveMatching()
    {
        // T027: Verify case-insensitive matching works
        var headerEntry = new Metadata.Entry("Content-Type", "application/grpc");
        headerEntry.Key.Equals("content-type", StringComparison.OrdinalIgnoreCase).Should().BeTrue();
        headerEntry.Key.Equals("CONTENT-TYPE", StringComparison.OrdinalIgnoreCase).Should().BeTrue();
    }

    #endregion
}
