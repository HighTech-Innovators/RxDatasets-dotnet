using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace RxDatasets.Abstractions
{
    public static class ConfigExtensions
    {
        // Hosts in one process bind their sections concurrently, so the set must be safe for that.
        private static readonly ConcurrentDictionary<string, byte> LoggedConfigKeys = new();
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
                // Claimed before writing, in one step, so that of two hosts binding the same section at once only
                // one writes its keys.
                if (!LoggedConfigKeys.TryAdd(key, 0))
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
