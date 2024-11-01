using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
public class GrpcWebApplicationFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.UseTestServer();

        builder.ConfigureServices(services =>
        {
            builder.UseEnvironment("Testing");
            builder.UseTestServer();
        });
    }
}
