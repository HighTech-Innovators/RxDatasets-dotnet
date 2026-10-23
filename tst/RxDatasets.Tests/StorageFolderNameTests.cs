using RxDatasets.Configs;

// Stub types in versioned namespaces, as protoc generates them with a csharp_namespace like Pricer.Evo.Dtos.LinkV2
namespace Contoso.Dtos.WidgetV1 { class Widget { } }
namespace Contoso.Dtos.WidgetV2 { class Widget { } }
namespace Contoso.Dtos.WidgetV0 { class Widget { } }
namespace Contoso.Dtos.Widgetv12 { class Widget { } }

namespace RxDatasets.Tests
{
    public class StorageFolderNameTests
    {
        [Theory]
        [InlineData(typeof(Contoso.Dtos.WidgetV1.Widget), "Widget")]
        [InlineData(typeof(Contoso.Dtos.Widget.V1.Widget), "Widget")]
        [InlineData(typeof(Contoso.Dtos.Gadget.Gadget), "Gadget")]
        [InlineData(typeof(Contoso.Dtos.WidgetV2.Widget), "WidgetV2")]
        [InlineData(typeof(Contoso.Dtos.Widget.V2.Widget), "WidgetV2")]
        [InlineData(typeof(Contoso.Dtos.WidgetV0.Widget), "WidgetV0")]
        [InlineData(typeof(Contoso.Dtos.Widgetv12.Widget), "WidgetV12")]
        [InlineData(typeof(Contoso.V2.Dtos.Widget.V1.Widget), "Widget")] // only the last namespace segment counts
        public void StorageFolderName_SuffixesEveryVersionButOne(System.Type type, string expected)
        {
            DatasetHelper.StorageFolderName(type).Should().Be(expected);
        }

        [Fact]
        public void GetObjectNamePrefix_UsesTheVersionedFolder_UnlessDtoNameIsSet()
        {
            DatasetHelper.GetObjectNamePrefix<Contoso.Dtos.WidgetV2.Widget>(new StorageConfig { CustomRootFolder = "root/" })
                .Should().Be("root/WidgetV2/");
            DatasetHelper.GetObjectNamePrefix<Contoso.Dtos.WidgetV2.Widget>(new StorageConfig { DtoName = "Custom" })
                .Should().Be("Custom/");
        }
    }
}
