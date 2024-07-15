using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace RxDatasets.Abstractions
{
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds and validates a configuration section.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="services">The service collection.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddValidatedConfigSection<TConfig>(this IServiceCollection services) where TConfig : class
        {
            string key = typeof(TConfig).Name.Replace("Config", "");
            return services.AddValidatedConfigSection<TConfig>(key);
        }

        /// <summary>
        /// Adds and validates a configuration section with a specified key.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="services">The service collection.</param>
        /// <param name="key">The configuration section key.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddValidatedConfigSection<TConfig>(this IServiceCollection services, string key) where TConfig : class
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException("Configuration section key cannot be null or empty.", nameof(key));
            }

            services.Configure<TConfig>(options =>
            {
                var configuration = services.BuildServiceProvider().GetRequiredService<IConfiguration>();
                configuration.GetSection(key).Bind(options);
            });

            services.AddSingleton(resolver =>
            {
                var options = resolver.GetRequiredService<IOptions<TConfig>>();
                return options.Value ?? throw new InvalidOperationException($"Configuration section '{key}' for '{typeof(TConfig)}' is not properly configured.");
            });

            services.AddSingleton<IOptionsMonitor<TConfig>, OptionsMonitor<TConfig>>();

            services.AddOptions<TConfig>()
                    .ValidateDataAnnotations()
                    .ValidateOnStart();

            return services;
        }
    }
}