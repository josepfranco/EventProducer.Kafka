using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Abstractions.EventProducer;
using Abstractions.EventProducer.Exceptions;
using Abstractions.Events.Models;
using Confluent.Kafka;
using EventProducer.Kafka.Configuration;
using Microsoft.Extensions.Options;

namespace EventProducer.Kafka
{
    public class Producer : IProducer, IProducerAsync
    {
        private ProducerConfig _producerConfig;
        
        public Producer(IOptions<KafkaProducerConfiguration> kafkaConfigOptions)
        {
            if (kafkaConfigOptions == null) 
                throw new ArgumentNullException(
                    nameof(KafkaProducerConfiguration), 
                    $"{nameof(KafkaProducerConfiguration)} options object not correctly setup.");
            
            var kafkaConfiguration = kafkaConfigOptions.Value;
            InitializeKafka(kafkaConfiguration);
        }

        /// <inheritdoc cref="IProducer.Produce"/>
        public void Produce(Event @event)
        {
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig).Build();
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
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig).Build();
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
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig).Build();
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
            using var producerBuilder = new ProducerBuilder<string, Event>(_producerConfig).Build();
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
                    ? throw new ArgumentException(
                        $"{nameof(KafkaProducerConfiguration)} bootstrap server urls cannot be null or empty.",
                        nameof(KafkaProducerConfiguration))
                    : kafkaConfiguration.BootstrapServerUrls
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
        protected virtual void Dispose(bool disposing)
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