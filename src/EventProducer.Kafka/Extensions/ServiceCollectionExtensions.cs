using Abstractions.EventProducer;
using Abstractions.Events;
using EventProducer.Kafka.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventProducer.Kafka.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaProducer(this IServiceCollection services,
                                                          IConfiguration configuration)
        {
            services.Configure<KafkaProducerConfiguration>(configuration.GetSection(nameof(KafkaProducerConfiguration)));
            services.AddTransient<Producer>();
            services.AddTransient<IProducer, Producer>(sp => sp.GetRequiredService<Producer>());
            services.AddTransient<IProducerAsync, Producer>(sp => sp.GetRequiredService<Producer>());
            services.AddTransient<IEventFactory, EventFactory>();
            return services;
        }
    }
}