using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Abstractions.EventProducer;
using Abstractions.EventProducer.Exceptions;
using Abstractions.Events.Models;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using EventProducer.Kafka.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace EventProducer.Kafka
{
    public sealed class Producer : IProducer, IProducerAsync
    {
        private readonly ILogger<Producer> _logger;
        
        private ProducerConfig? _producerConfig;
        private SchemaRegistryConfig? _schemaRegistryConfig;
        
        public Producer(ILogger<Producer> logger, IOptions<KafkaProducerConfiguration> kafkaConfigOptions)
        {
            _logger = logger;
            var kafkaConfiguration = kafkaConfigOptions.Value;
            InitializeKafka(kafkaConfiguration);
        }

        /// <inheritdoc cref="IProducer.Produce"/>
        public void Produce(Event @event)
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig)
               .SetKeySerializer(Serializers.Utf8)
               .SetValueSerializer(new AvroSerializer<Event>(schemaRegistry))
               .SetErrorHandler((_, e) => _logger.LogError("Serialization failed error: {Reason}", e.Reason))
               .Build();
            var message = new Message<string, Event>
            {
                Key = @event.AggregateName,
                Value = @event
            };
            
            producerBuilder.Produce(@event.AggregateName, message, report =>
            {
                if (report.Error.IsFatal) 
                    throw new ProducerException("Fatal error producing message to Kafka: " +
                                                $"ErrorCode [{report.Error.Code}] | " +
                                                $"Reason [{report.Error.Reason}]");
            });

            // wait for up to 5 seconds for any inflight messages to be delivered.
            producerBuilder.Flush(TimeSpan.FromSeconds(10));
        }

        /// <inheritdoc cref="IProducerAsync.ProduceAsync"/>
        public Task ProduceAsync(Event @event, CancellationToken token = default)
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig)
               .SetKeySerializer(Serializers.Utf8)
               .SetValueSerializer(new AvroSerializer<Event>(schemaRegistry))
               .SetErrorHandler((_, e) => _logger.LogError("Serialization failed error: {Reason}", e.Reason))
               .Build();
            var message = new Message<string, Event>
            {
                Key = @event.AggregateName,
                Value = @event
            };

            try
            {
                return producerBuilder.ProduceAsync(@event.AggregateName, message, token);
            }
            catch (Exception e)
            {
                throw new ProducerException(e.Message, e);
            }
        }

        /// <inheritdoc cref="IProducer.ProduceMany"/>
        public void ProduceMany(IList<Event> events)
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig)
               .SetKeySerializer(Serializers.Utf8)
               .SetValueSerializer(new AvroSerializer<Event>(schemaRegistry))
               .SetErrorHandler((_, e) => _logger.LogError("Serialization failed error: {Reason}", e.Reason))
               .Build();
            foreach (var @event in events)
            {
                var message = new Message<string, Event>
                {
                    Key = @event.AggregateName,
                    Value = @event
                };
            
                producerBuilder.Produce(@event.AggregateName, message, report =>
                {
                    if (report.Error.IsFatal) 
                        throw new ProducerException("Fatal error producing message to Kafka: " +
                                                    $"ErrorCode [{report.Error.Code}] | " +
                                                    $"Reason [{report.Error.Reason}]");
                });
            }
            // wait for up to 5 seconds for any inflight messages to be delivered.
            producerBuilder.Flush(TimeSpan.FromSeconds(10));
        }

        /// <inheritdoc cref="IProducerAsync.ProduceManyAsync"/>
        public async Task ProduceManyAsync(IList<Event> events, CancellationToken token = default)
        {
            using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig)
               .SetKeySerializer(Serializers.Utf8)
               .SetValueSerializer(new AvroSerializer<Event>(schemaRegistry))
               .SetErrorHandler((_, e) => _logger.LogError("Serialization failed error: {Reason}", e.Reason))
               .Build();
            foreach (var @event in events)
            {
                var message = new Message<string, Event>
                {
                    Key = @event.AggregateName,
                    Value = @event
                };
            
                try
                {
                    await producerBuilder.ProduceAsync(@event.AggregateName, message, token);
                }
                catch (Exception e)
                {
                    throw new ProducerException(e.Message, e);
                }
            }
        }

        #region PRIVATE METHODS
        private void InitializeKafka(KafkaProducerConfiguration kafkaConfiguration)
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = string.IsNullOrEmpty(kafkaConfiguration.BootstrapServerUrls)
                    ? throw new InvalidOperationException(
                        $"{nameof(KafkaProducerConfiguration)} bootstrap server urls cannot be null or empty.")
                    : kafkaConfiguration.BootstrapServerUrls,
            };
            _schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = string.IsNullOrEmpty(kafkaConfiguration.AvroSchemaRegistryUrls)
                    ? throw new InvalidOperationException(
                        $"{nameof(KafkaProducerConfiguration)} schema registry urls cannot be null or empty.")
                    : kafkaConfiguration.AvroSchemaRegistryUrls
            };
        }
        #endregion

        #region DISPOSE PATTERN
        private bool _disposed;
        
        /**
         * Consumer dispose method
         */
        public void Dispose()
        {
            Dispose(true);
        }
        
        /**
         * How we dispose an object
         */
        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                // to be implemented if needed
            }
            _disposed = true;
        }
        #endregion
    }
}