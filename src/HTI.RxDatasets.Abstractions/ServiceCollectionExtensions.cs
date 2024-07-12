using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.ComponentModel.DataAnnotations;

namespace HTI.RxDatasets.Abstractions
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

    public static class ConfigurationExtensions
    {
        /// <summary>
        /// Gets and binds a configuration section.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="configuration">The configuration instance.</param>
        /// <returns>The configuration class instance.</returns>
        public static TConfig GetConfigSection<TConfig>(this IConfiguration configuration) where TConfig : class
        {
            string key = typeof(TConfig).Name.Replace("Config", "");
            return configuration.GetConfigSection<TConfig>(key);
        }

        /// <summary>
        /// Gets and binds a configuration section with a specified key.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="configuration">The configuration instance.</param>
        /// <param name="key">The configuration section key.</param>
        /// <returns>The configuration class instance.</returns>
        public static TConfig GetConfigSection<TConfig>(this IConfiguration configuration, string key) where TConfig : class
        {
            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentException("Configuration section key cannot be null or empty.", nameof(key));
            }

            var section = configuration.GetSection(key) ?? throw new Exception($"Configuration section '{key}' is missing.");
            var config = section.Get<TConfig>() ?? throw new Exception($"Couldn't bind configuration section '{key}' to type '{typeof(TConfig)}'.");
            ValidateConfig(config);

            return config;
        }

        /// <summary>
        /// Validates the configuration using data annotations.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="config">The configuration instance.</param>
        private static void ValidateConfig<TConfig>(TConfig config) where TConfig : class
        {
            var validationContext = new ValidationContext(config);
            Validator.ValidateObject(config, validationContext, validateAllProperties: true);
        }
    }
}