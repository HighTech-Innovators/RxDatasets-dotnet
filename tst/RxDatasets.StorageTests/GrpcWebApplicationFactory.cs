using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
public class GrpcWebApplicationFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.UseTestServer();

        builder.ConfigureServices(_ =>
        {
            builder.UseEnvironment("Testing");
            builder.UseTestServer();
        });
    }
}
