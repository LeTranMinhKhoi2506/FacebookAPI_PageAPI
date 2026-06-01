using Confluent.Kafka;
using System.Text.Json;

namespace RetryService.Services
{
    public class DeadLetterService : IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private readonly ILogger<DeadLetterService> _logger;
        private readonly string _deadLetterTopic;

        public DeadLetterService(IConfiguration configuration, ILogger<DeadLetterService> logger)
        {
            _logger = logger;
            _deadLetterTopic = configuration["Kafka:DeadLetterTopic"] ?? "dead_letter";

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task PublishDeadLetterAsync(string commandId, string originalPayload, string reason, int attemptCount, string? sourceTopic = null)
        {
            try
            {
                var deadLetterEvent = new
                {
                    deadLetterId = Guid.NewGuid().ToString(),
                    commandId = commandId,
                    sourceTopic = sourceTopic,
                    originalPayload = originalPayload,
                    failureReason = reason,
                    attemptCount = attemptCount,
                    failedAt = DateTime.UtcNow,
                    status = "dead_letter"
                };

                var json = JsonSerializer.Serialize(deadLetterEvent);

                await _producer.ProduceAsync(_deadLetterTopic, new Message<string, string>
                {
                    Key = commandId,
                    Value = json
                });

                _logger.LogWarning("Published message to dead letter topic. CommandId={CommandId}, Topic={Topic}, Reason={Reason}",
                    commandId, _deadLetterTopic, reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish to dead letter topic for CommandId: {CommandId}", commandId);
            }
        }

        public void Dispose()
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
        }
    }
}
