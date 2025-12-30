using Google.Protobuf;
using RxDatasets.Aliasing;
using RxDatasets.AliasingTests.Types.Relation;

namespace RxDatasets.AliasingTests;

/// <summary>
/// Low-level unit tests for EvaluateJsonPath method.
/// Tests the path navigation and slicing logic in isolation.
/// </summary>
public class EvaluateJsonPathTests
{
    private static byte[] RelationToBytes(Relation relation) => relation.ToByteArray();

    // ==================== SIMPLE FIELD ACCESS ====================

    [Fact]
    public void EvaluateJsonPath_Should_Get_Simple_Field()
    {
        // Test: .id should return the ID field
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id}";

        var relation = new Relation { Id = "t/100/s/200/relations/abc" };
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100/s/200/relations/abc");
    }

    [Fact]
    public void EvaluateJsonPath_Should_Navigate_Nested_Fields()
    {
        // Test: .single.related_product.product_id should navigate through nested objects
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.single.related_product.product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/single1",
            Single = new Relation.Types.Single
            {
                RelatedProduct = new Relation.Types.RelatedProduct { ProductId = "product-123" }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("product-123");
    }

    // ==================== SINGLE INDEX [0] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Get_First_Product_With_Index_0()
    {
        // Test: related_products[0].product_id should return only the first product
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.multi_product.related_products[0].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first" },
                    new Relation.Types.RelatedProduct { ProductId = "second" },
                    new Relation.Types.RelatedProduct { ProductId = "third" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("first");
    }

    [Fact]
    public void EvaluateJsonPath_Should_Get_Last_Product_With_Negative_Index()
    {
        // Test: related_products[2].product_id should return the product at index 2
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.multi_product.related_products[2].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "first" },
                    new Relation.Types.RelatedProduct { ProductId = "second" },
                    new Relation.Types.RelatedProduct { ProductId = "third" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("third");
    }

    // ==================== RANGE SLICING [M..N] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Slice_Range_M_To_N()
    {
        // Test: related_products[1..3].product_id should return products at indices 1 and 2
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

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/second");
        aliases.Should().Contain("product/third");
    }

    // ==================== LEADING SLICE [..N] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Slice_Leading_First_N()
    {
        // Test: related_products[..2].product_id should return first 2 products
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

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/first");
        aliases.Should().Contain("product/second");
    }

    // ==================== TRAILING SLICE [N..] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Slice_Trailing_From_N_To_End()
    {
        // Test: related_products[2..].product_id should return products from index 2 to end
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

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().HaveCount(2);
        aliases.Should().Contain("product/third");
        aliases.Should().Contain("product/fourth");
    }

    // ==================== FULL ARRAY ITERATION [] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Iterate_All_Array_Products()
    {
        // Test: related_products[].product_id should return all products
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "product/{.multi_product.related_products[].product_id}";

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "product-a" },
                    new Relation.Types.RelatedProduct { ProductId = "product-b" },
                    new Relation.Types.RelatedProduct { ProductId = "product-c" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().HaveCount(3);
        aliases.Should().Contain("product/product-a");
        aliases.Should().Contain("product/product-b");
        aliases.Should().Contain("product/product-c");
    }

    [Fact]
    public void EvaluateJsonPath_Should_Deduplicate_Only_At_Factory_Level()
    {
        // Test: When multiple patterns produce same alias, only one should appear
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var pattern1 = "product/{.multi_product.related_products[0].product_id}";
        var pattern2 = "product/{.multi_product.related_products[0].product_id}"; // Same pattern

        var relation = new Relation
        {
            Id = "t/100/s/200/relations/multi1",
            MultiProduct = new Relation.Types.MultiProduct
            {
                RelatedProducts =
                {
                    new Relation.Types.RelatedProduct { ProductId = "product-1" },
                    new Relation.Types.RelatedProduct { ProductId = "product-2" }
                }
            }
        };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { pattern1, pattern2 },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        // Should be deduplicated at factory level, so only 1 alias
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("product/product-1");
    }

    // ==================== STRING VALUE SLICING /[..N] ====================

    [Fact]
    public void EvaluateJsonPath_Should_Slice_ID_String_First_N_Parts()
    {
        // Test: id/[..2] should split by "/" and take first 2 parts
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[..2]}";

        var relation = new Relation { Id = "t/100/s/200/relations/abc" };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("t/100");
    }

    [Fact]
    public void EvaluateJsonPath_Should_Slice_ID_String_Range()
    {
        // Test: id/[1..3] should split by "/" and take parts at indices 1 and 2
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[1..3]}";

        var relation = new Relation { Id = "t/100/s/200/relations/abc" };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("100/s");
    }

    [Fact]
    public void EvaluateJsonPath_Should_Slice_ID_String_From_Index()
    {
        // Test: id/[2..] should split by "/" and take from index 2 to end
        var idPattern = "t/{tokenid}/s/{systemid}/relations/{relation-id}";
        var aliasPattern = "{.id/[2..]}";

        var relation = new Relation { Id = "t/100/s/200/relations/abc" };

        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(
            idPattern,
            new List<string> { aliasPattern },
            Relation.Parser);

        var aliases = factory(RelationToBytes(relation)).ToList();
        aliases.Should().ContainSingle();
        aliases[0].Should().Be("s/200/relations/abc");
    }
}
