using System;
using Abstractions.Events;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EventProducer.Kafka.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventProducer.Kafka
{
    /// <inheritdoc cref="IEventFactory"/>
    public class EventFactory : IEventFactory
    {
        private readonly SchemaRegistryConfig _schemaRegistryConfig;

        public EventFactory(IOptions<KafkaProducerConfiguration> kafkaConfigOptions)
        {
            var kafkaConfiguration = kafkaConfigOptions.Value;

            _schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = kafkaConfiguration.AvroSchemaRegistryUrls
            };
        }

        /// <inheritdoc cref="IEventFactory.Create{TEvent}"/>
        public Event Create<TEvent>(string aggregateName, TEvent? payload = null) where TEvent : class
        {
            if (string.IsNullOrWhiteSpace(aggregateName)) throw new ArgumentNullException(nameof(payload));

            // serialize payload
            if (payload is null)
            {
                return new Event
                {
                    Name          = typeof(TEvent).Name,
                    AggregateName = aggregateName,
                    CreatedAt     = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                };
            }

            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);

            var specificRecord = payload as ISpecificRecord ??
                                 throw new InvalidOperationException(
                                     $"Inserted payload must be a subtype of {nameof(ISpecificRecord)}");

            var serializer           = new AvroSerializer<TEvent>(schemaRegistry).AsSyncOverAsync();
            var serializationContext = new SerializationContext(MessageComponentType.Value, aggregateName);
            var payloadData          = serializer.Serialize(payload, serializationContext);
            return new Event
            {
                Name          = typeof(TEvent).Name,
                AggregateName = aggregateName,
                CreatedAt     = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Payload = new EventPayload
                {
                    DataSchema = specificRecord.Schema.ToString(),
                    Data       = payloadData
                }
            };

        }
    }
}