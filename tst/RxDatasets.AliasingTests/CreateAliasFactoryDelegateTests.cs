using RxDatasets.AliasingTests.Types.Relation;
using Google.Protobuf;
using RxDatasets.Aliasing;

namespace RxDatasets.AliasingTests;

/// <summary>
/// Unit tests for the CreateAliasFactoryDelegate method.
/// Tests the pattern matching engine that converts protobuf entities into aliases.
/// </summary>
public class CreateAliasFactoryDelegateTests
{
    /// <summary>
    /// Helper to create a Relation and serialize it to bytes for testing
    /// </summary>
    private static byte[] RelationToBytes(Relation relation) => relation.ToByteArray();

    // ==================== BASIC VARIABLE EXTRACTION ====================

    [Fact]
    public void Should_Extract_Variables_From_IdPattern()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "t/{tokenid}/s/{systemid}/product/{.id}/relation";

        var relation = new Relation { Id = "t/100/s/200/relations/abc" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100/s/200/product/t/100/s/200/relations/abc/relation");
    }

    [Fact]
    public void Should_Handle_Single_Variable()
    {
        // Arrange
        var idPattern = "product/{productid}";
        var aliasPattern = "by-product/{productid}";

        var relation = new Relation { Id = "product/12345" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("by-product/12345");
    }

    [Fact]
    public void Should_Handle_Multiple_Variables()
    {
        // Arrange
        var idPattern = "tenant/{tokenid}/store/{systemid}/product/{productid}";
        var aliasPattern = "t/{tokenid}/s/{systemid}/i/{productid}";

        var relation = new Relation { Id = "tenant/100/store/200/product/abc" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100/s/200/i/abc");
    }

    // ==================== FIELD ACCESS (JSON SYNTAX) ====================

    [Fact]
    public void Should_Access_Simple_Field()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "all/{.id}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("all/t/100/s/200/relations/xyz");
    }

    [Fact]
    public void Should_Access_Nested_Field()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.single.related_product.product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            Single = new Relation.Types.Single
            {
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "product-999" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("product/product-999");
    }

    [Fact]
    public void Should_Skip_Alias_When_Field_Missing()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.single.related_product.product_id}";

        // Relation without single variant set
        var relation = new Relation { Id = "t/100/s/200/relations/1" };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - alias should be skipped because the field doesn't exist
        aliases.Should().BeEmpty();
    }

    // ==================== ARRAY ITERATION ====================

    [Fact]
    public void Should_Iterate_Array_And_Get_First_Product()
    {
        // Arrange - test [0] syntax to access first element by index
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[0].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first-product" },
                    new Relation.Types.RelatedProduct { ProductId = "second-product" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [0] syntax accesses the element at index 0 explicitly
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("product/first-product");
    }

    [Fact]
    public void Should_Skip_Alias_When_Array_Empty()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            MultiProduct = new Relation.Types.MultiProduct()
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Iterate_Through_Repeated_Field_And_Get_All_Non_Empty()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "device_id/{.multi_device.devices[].device_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            MultiDevice = new Relation.Types.MultiDevice
            {
                Devices =
                {
                    new Relation.Types.Device { DeviceId = "device_id1" },
                    new Relation.Types.Device { DeviceId = "" },
                    new Relation.Types.Device { DeviceId = "device_id2" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [] operator returns all non-empty products
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("device_id/device_id1");
        aliases.Should().Contain("device_id/device_id2");
    }

    [Fact]
    public void Should_Skip_Alias_When_All_Array_Products_Empty()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "device_id/{.multi_device.devices[].device_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            MultiDevice = new Relation.Types.MultiDevice
            {
                Devices =
                {
                    new Relation.Types.Device { DeviceId = "" },
                    new Relation.Types.Device { DeviceId = "" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - should skip if all products have empty values
        aliases.Should().BeEmpty();
    }

    // ==================== SLICING OPERATOR ====================

    [Fact]
    public void Should_Apply_Slicing_Operator()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[..4]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - should take first 4 parts: t/100/s/200
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100/s/200");
    }

    [Fact]
    public void Should_Handle_Slicing_When_Parts_Less_Than_Requested()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[..10]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - should return all parts when there are fewer than requested
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100/s/200/relations/xyz");
    }

    [Fact]
    public void Should_Apply_Slicing_With_Different_Counts()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "{.id/[..2]}",
            "{.id/[..3]}",
            "{.id/[..5]}"
        };

        var relation = new Relation { Id = "a/b/c/d/e/f" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(3);
        aliases.Should().Contain("a/b");
        aliases.Should().Contain("a/b/c");
        aliases.Should().Contain("a/b/c/d/e");
    }

    // ==================== MULTIPLE ALIAS PATTERNS ====================

    [Fact]
    public void Should_Generate_Multiple_Aliases()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "product/{.single.related_product.product_id}",
            "device_id/{.single.device.device_id}"
        };

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            Single = new Relation.Types.Single
            {
                Device = new Relation.Types.Device { DeviceId = "device_id123" },
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "product456" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/product456");
        aliases.Should().Contain("device_id/device_id123");
    }

    [Fact]
    public void Should_Skip_Invalid_Pattern_But_Keep_Valid_Ones()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "product/{.single.related_product.product_id}",
            "device_id/{.multi_device.related_product.product_id}" // Will fail - multi_device not set
        };

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            Single = new Relation.Types.Single
            {
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "product456" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - only the valid one should be included
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("product/product456");
    }

    // ==================== COMPLEX REAL-WORLD PATTERNS ====================

    [Fact]
    public void Should_Handle_Complex_Relation_Alias_Pattern()
    {
        // Arrange - simulate the RelationV1 DI configuration
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "t/{tokenid}/s/{systemid}/product/{.single.related_product.product_id}/relations/{relation-id}";

        var relation = new Relation
        {
            Id = "t/acme/s/store5/relations/link123",
            Single = new Relation.Types.Single
            {
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "product-789" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/acme/s/store5/product/product-789/relations/link123");
    }

    [Fact]
    public void Should_Generate_All_RelationV1_Alias_Types()
    {
        // Arrange - simulate all RelationV1 alias patterns
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "{.id/[..4]}/product/{.single.related_product.product_id}/relations/{relation-id}",
            "t/{tokenid}/s/{systemid}/product/{.single.related_product.product_id}/relations/{relation-id}",
            "t/{tokenid}/s/{systemid}/device/{.single.device.device_id}/relation"
        };

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/xyz",
            Single = new Relation.Types.Single
            {
                Device = new Relation.Types.Device { DeviceId = "4006381333931" },
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "1234567890123" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - first pattern is deduped with second (both generate same alias), third is unique
        aliases.Should().HaveCount(2);
        // Both first and second patterns generate: "t/100/s/200/product/1234567890123/relations/xyz" (deduped to one)
        aliases.Should().Contain("t/100/s/200/product/1234567890123/relations/xyz");
        // Third pattern generates unique alias
        aliases.Should().Contain("t/100/s/200/device/4006381333931/relation");
    }

    [Fact]
    public void Should_Handle_Multi_Product_Relation_Alias()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "t/{tokenid}/s/{systemid}/product/{.multi_product.related_products[].product_id}/relations/{relation-id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "multi-product-1" },
                    new Relation.Types.RelatedProduct { ProductId = "multi-product-2" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [] operator returns first and second products (generates 2 aliases)
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("t/100/s/200/product/multi-product-1/relations/multi1");
        aliases.Should().Contain("t/100/s/200/product/multi-product-2/relations/multi1");
    }

    [Fact]
    public void Should_Handle_Floating_Canvas_Relation_Alias()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "t/{tokenid}/s/{systemid}/product/{.multi_device.related_product.product_id}/relations/{relation-id}",
            "t/{tokenid}/s/{systemid}/product/{.multi_device.devices[].device_id}/relation"
        };

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/float1",
            MultiDevice = new Relation.Types.MultiDevice
            {
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "float-product" },
                Devices =
                {
                    new Relation.Types.Device { DeviceId = "float-device_id-1" },
                    new Relation.Types.Device { DeviceId = "float-device_id-2" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - first pattern generates 1, second pattern generates 2 (for the two devices in range operator)
        aliases.Should().HaveCount(3);
        aliases.Should().Contain("t/100/s/200/product/float-product/relations/float1");
        aliases.Should().Contain("t/100/s/200/product/float-device_id-1/relation");
        aliases.Should().Contain("t/100/s/200/product/float-device_id-2/relation");
    }

    // ==================== EDGE CASES ====================

    [Fact]
    public void Should_Return_Empty_When_Entity_Id_Is_Empty()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id}";

        var relation = new Relation { Id = "" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Handle_Invalid_Proto_Data_Gracefully()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id}";

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act - pass invalid proto bytes
        var aliases = factory("", new byte[] { 0xFF, 0xFF, 0xFF }).ToList();

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Return_Empty_When_No_Patterns_Provided()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string>(),
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Handle_Pattern_With_Literal_Text_Only()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "static/alias/path";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("static/alias/path");
    }

    [Fact]
    public void Should_Handle_Mixed_Literal_And_Variable_Text()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "prefix-{tokenid}-suffix";

        var relation = new Relation { Id = "t/tenant1/s/store1/relations/link1" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("prefix-tenant1-suffix");
    }

    [Fact]
    public void Should_Return_Empty_When_Variable_Not_Found_In_Id()
    {
        // Arrange
        var idPattern = "product/{productid}";
        var aliasPattern = "tenant-{tokenid}/product/{productid}"; // {tokenid} doesn't exist in ID

        var relation = new Relation { Id = "product/123" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Handle_Very_Deep_Nesting()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "device-type/{.single.device.model_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/1",
            Single = new Relation.Types.Single
            {
                Device = new Relation.Types.Device { ModelId = "devicetypes/display-42" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("device-type/devicetypes/display-42");
    }

    [Fact]
    public void Should_Handle_Slicing_With_Single_Part()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[..1]}";

        var relation = new Relation { Id = "only-one-part" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("only-one-part");
    }

    [Fact]
    public void Should_Handle_Zero_Slice_Count()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[..0]}";

        var relation = new Relation { Id = "a/b/c" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - taking first 0 parts gives empty string which cancels the alias
        aliases.Should().BeEmpty();
    }

    // ==================== RANGE SLICING [M..N] ====================

    [Fact]
    public void Should_Apply_Range_Slicing_Basic()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[1..3]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [1..3] means start at index 1, end before index 3: "100/s"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("100/s");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_From_Start()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[0..2]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [0..2] means start at index 0, end before index 2: "t/100"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_To_End()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[3..]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [3..] means start at index 3 and go to end: "200/relations/xyz"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("200/relations/xyz");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Single_Element()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[2]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [2..3] means start at index 2, end before index 3 (single element): "s"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("s");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Single_Element_Outside_Valid_Range()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[20]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [20] is out of bounds, so the alias pattern should be skipped entirely
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Single_Element_Range()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[2..3]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [2..3] means start at index 2, end before index 3 (single element): "s"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("s");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Empty_Range()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[2..2]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - empty range produces empty string which cancels the alias
        aliases.Should().BeEmpty();
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Out_Of_Bounds_End()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[3..100]}";

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - should take from index 3 to end: "200/relations/xyz"
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("200/relations/xyz");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_Multiple_Patterns()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "{.id/[0..2]}",   // "t/100"
            "{.id/[1..4]}",   // "100/s/200"
            "{.id/[4..]}"     // "relations/xyz"
        };

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(3);
        aliases.Should().Contain("t/100");
        aliases.Should().Contain("100/s/200");
        aliases.Should().Contain("relations/xyz");
    }

    [Fact]
    public void Should_Handle_Range_Slicing_With_Mixed_Patterns()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "prefix/{.id/[1..3]}/suffix",           // "prefix/b/c/suffix"
            "prefix/{.id/[..2]}/middle/{.id/[4..]}" // "prefix/a/b/middle/e/f"
        };

        var relation = new Relation { Id = "a/b/c/d/e/f" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("prefix/b/c/suffix");
        aliases.Should().Contain("prefix/a/b/middle/e/f");
    }

    [Fact]
    public void Should_Skip_Pattern_When_Start_Index_Out_Of_Bounds()
    {
        // Arrange
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[10..20]}";  // Out of bounds

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - should skip the pattern (or return empty string depending on implementation)
        // This test documents expected behavior for out-of-bounds ranges
        // Currently expected to either skip or return empty
        aliases.Should().BeEmpty("or contain empty string - implementation choice");
    }

    [Fact]
    public void Should_Combine_Range_Slicing_With_Variables()
    {
        // Arrange - real-world example with RelationV1 pattern
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            // [0..2] gets indices 0-1: "t/100"
            "{.id/[0..2]}/custom/{relation-id}",
            // [3..5] gets indices 3-4: "200/relations"
            "store/{systemid}/range/{.id/[3..5]}"
        };

        var relation = new Relation { Id = "t/100/s/200/relations/xyz" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("t/100/custom/xyz");
        aliases.Should().Contain("store/200/range/200/relations");
    }

    [Fact]
    public void Should_Handle_Open_End_Slicing()
    {
        // Arrange - test [N..] syntax for "from N to end"
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPatterns = new List<string>
        {
            "{.id/[1..]}",   // From index 1 to end: "b/c/d/e/f/g"
            "{.id/[0..]}",   // From index 0 to end: "a/b/c/d/e/f/g"
            "{.id/[5..]}"    // From index 5 to end: "f/g"
        };

        var relation = new Relation { Id = "a/b/c/d/e/f/g" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            aliasPatterns,
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert
        aliases.Should().HaveCount(3);
        aliases.Should().Contain("b/c/d/e/f/g");
        aliases.Should().Contain("a/b/c/d/e/f/g");
        aliases.Should().Contain("f/g");
    }

    [Fact]
    public void Should_Not_Duplicate_Aliases_When_Same_Product_Appears_Multiple_Times_In_Array()
    {
        // Arrange - multi-product relation with the same product appearing twice, plus a different product
        // Verify that [] generates aliases for all products, but deduplicates identical values
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "hagelslag400g" },
                    new Relation.Types.RelatedProduct { ProductId = "hagelslag400g" },
                    new Relation.Types.RelatedProduct { ProductId = "stroopwafel500g" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [] operator generates aliases for all distinct products
        // hagelslag400g appears twice but should only generate one alias (deduplication)
        // stroopwafel500g generates another alias
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/hagelslag400g");
        aliases.Should().Contain("product/stroopwafel500g");
    }

    [Fact]
    public void Should_Apply_Range_Slicing_To_Array_Aliases()
    {
        // Arrange - test [M..N] slicing on array iteration
        // Verify that [] generates multiple aliases and [M..N] can slice them
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[1..3].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first" },
                    new Relation.Types.RelatedProduct { ProductId = "second" },
                    new Relation.Types.RelatedProduct { ProductId = "third" },
                    new Relation.Types.RelatedProduct { ProductId = "fourth" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [1..3] should yield elements at indices 1 and 2 (second and third products)
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/second");
        aliases.Should().Contain("product/third");
    }

    [Fact]
    public void Should_Apply_Leading_Slice_To_Array_Aliases()
    {
        // Arrange - test [..N] slicing (first N products)
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[..2].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first" },
                    new Relation.Types.RelatedProduct { ProductId = "second" },
                    new Relation.Types.RelatedProduct { ProductId = "third" },
                    new Relation.Types.RelatedProduct { ProductId = "fourth" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [..2] should yield first 2 products (indices 0 and 1)
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/first");
        aliases.Should().Contain("product/second");
    }

    [Fact]
    public void Should_Apply_Trailing_Slice_To_Array_Aliases()
    {
        // Arrange - test [N..] slicing (skip N products, take rest)
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[2..].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first" },
                    new Relation.Types.RelatedProduct { ProductId = "second" },
                    new Relation.Types.RelatedProduct { ProductId = "third" },
                    new Relation.Types.RelatedProduct { ProductId = "fourth" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        // Act
        var aliases = factory(relation.Id, RelationToBytes(relation)).ToList();

        // Assert - [2..] should yield from index 2 to end (third and fourth products)
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/third");
        aliases.Should().Contain("product/fourth");
    }
}
