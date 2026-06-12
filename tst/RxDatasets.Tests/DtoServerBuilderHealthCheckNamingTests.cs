// Stub types used to simulate same-named DTOs in different versioned namespaces.
namespace Contoso.Dtos.Widget.V1 { class Widget { } }
namespace Contoso.Dtos.Widget.V2 { class Widget { } }
namespace Contoso.Dtos.Gadget { class Gadget { } }
namespace Contoso.V2.Dtos.Widget.V1 { class Widget { } }
namespace Contoso.Dtos.Widgetv3 { class Widget { } }

namespace RxDatasets.Tests
{
    public class DtoServerBuilderHealthCheckNamingTests
    {
        [Fact]
        public void VersionedEntityName_AppendsVersionSuffix_WhenNamespaceContainsVersion()
        {
            DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.Dtos.Widget.V1.Widget))
                .Should().Be("Widget-V1");
        }

        [Fact]
        public void VersionedEntityName_DifferentiatesTwoVersions()
        {
            string v1 = DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.Dtos.Widget.V1.Widget));
            string v2 = DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.Dtos.Widget.V2.Widget));

            v1.Should().Be("Widget-V1");
            v2.Should().Be("Widget-V2");
            v1.Should().NotBe(v2);
        }

        [Fact]
        public void VersionedEntityName_ReturnsTypeName_WhenNoVersionInNamespace()
        {
            DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.Dtos.Gadget.Gadget))
                .Should().Be("Gadget");
        }

        [Fact]
        public void VersionedEntityName_UsesLastVersionSegment_WhenMultipleVersionsInNamespace()
        {
            // Namespace is Contoso.V2.Dtos.Widget.V1 — last match wins
            DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.V2.Dtos.Widget.V1.Widget))
                .Should().Be("Widget-V1");
        }

        [Fact]
        public void VersionedEntityName_PreservesLowercaseV()
        {
            DtoServerBuilder<Google.Protobuf.WellKnownTypes.Empty>.VersionedEntityName(typeof(Contoso.Dtos.Widgetv3.Widget))
                .Should().Be("Widget-v3");
        }
    }
}
