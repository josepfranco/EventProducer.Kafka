using System;
using Abstractions.Events;
using Abstractions.Events.Models;

namespace EventProducer.Kafka
{
    /// <inheritdoc cref="IEventFactory"/>
    public class EventFactory : IEventFactory
    {
        /// <inheritdoc cref="IEventFactory.Create{TEvent}"/>
        public Event Create<TEvent>(string aggregateName, TEvent? payload = null) where TEvent : class
        {
            if (string.IsNullOrWhiteSpace(aggregateName)) throw new ArgumentNullException(nameof(payload));
            return new Event
            {
                Name          = typeof(TEvent).Name,
                AggregateName = aggregateName,
                CreatedAt     = DateTime.UtcNow,
                Payload       = payload
            };
        }
    }
}