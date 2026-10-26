using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace RxDatasets.Abstractions
{
    public static class ConfigExtensions
    {
        private static readonly HashSet<string> LoggedConfigKeys = [];
        /// <summary>
        /// Adds and validates a configuration section.
        /// </summary>
        /// <typeparam name="TConfig">The configuration class type.</typeparam>
        /// <param name="services">The service collection.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddValidatedConfigSection<TConfig>(this IServiceCollection services) where TConfig : class
        {
            var key = typeof(TConfig).Name.Replace("Config", "");
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
                if (LoggedConfigKeys.Contains(key))
                    return;
                var configSection = configuration.GetSection(key).GetChildren();
                foreach (var child in configSection)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.Write("info: ");

                    // Reset the color to the default
                    Console.ResetColor();
                    // The key only, never the value: a section can hold a secret (a storage provider's
                    // SecretKey or ConnectionString), and stdout is the container log (VLOW-37).
                    Console.WriteLine($"{typeof(TConfig).Namespace}.{typeof(TConfig).Name} \n      Key: {key}__{child.Key}");
                }

                LoggedConfigKeys.Add(key);
            });

            services.AddSingleton(resolver => resolver.GetRequiredService<IOptions<TConfig>>().Value);

            services.AddSingleton<IOptionsMonitor<TConfig>, OptionsMonitor<TConfig>>();

            services.AddOptions<TConfig>()
                    .ValidateDataAnnotations()
                    .ValidateOnStart();

            return services;
        }
    }
}
