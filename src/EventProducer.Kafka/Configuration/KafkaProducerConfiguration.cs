using System;

namespace EventProducer.Kafka.Configuration
{
    public class KafkaProducerConfiguration
    {
        public string BootstrapServerUrls { get; set; } = string.Empty;
        
        public string AvroSchemaRegistryUrls { get; set; } = string.Empty;
    }
}