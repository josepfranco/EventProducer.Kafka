using Abstractions.EventProducer;
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
            services.AddSingleton(ServiceDescriptor.Singleton<IProducer, Producer>());
            services.AddSingleton(ServiceDescriptor.Singleton<IProducerAsync, Producer>());
            return services;
        }
    }
}