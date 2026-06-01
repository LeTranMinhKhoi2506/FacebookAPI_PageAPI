using Confluent.Kafka;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using RetryService.Data;
using RetryService.Models;

namespace RetryService.Services
{
    public class RetryConsumerService : BackgroundService
    {
        private readonly ILogger<RetryConsumerService> _logger;
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly IProducer<string, string> _producer;
        private readonly string _failedTopic;
        private readonly string _defaultRetryTopic;
        private readonly int _maxRetries;
        private readonly int[] _backoffSeconds;
        private readonly int _pollIntervalMs;
        private readonly int _circuitBreakerThreshold;
        private readonly int _circuitBreakerCooldownSeconds;
        private int _consecutiveDownstreamFailures;
        private DateTimeOffset? _circuitOpenUntil;
        private readonly DeadLetterService _deadLetterService;
        private readonly RetryRepository _retryRepository;
        private readonly AlertManager _alertManager;

        public RetryConsumerService(
            ILogger<RetryConsumerService> logger,
            IConfiguration configuration,
            DeadLetterService deadLetterService,
            RetryRepository retryRepository,
            AlertManager alertManager)
        {
            _logger = logger;
            _deadLetterService = deadLetterService;
            _retryRepository = retryRepository;
            _alertManager = alertManager;

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            _failedTopic = configuration["Kafka:FailedTopic"] ?? "send_failed";
            _defaultRetryTopic = configuration["Kafka:RetryTargetTopic"] ?? configuration["Kafka:Topic"] ?? "raw_events";
            _maxRetries = configuration.GetValue<int>("Retry:MaxAttempts", 3);
            _pollIntervalMs = configuration.GetValue<int>("Retry:PollIntervalMs", 1000);
            _circuitBreakerThreshold = configuration.GetValue<int>("Retry:CircuitBreakerThreshold", 5);
            _circuitBreakerCooldownSeconds = configuration.GetValue<int>("Retry:CircuitBreakerCooldownSeconds", 30);

            var backoffConfig = configuration.GetSection("Retry:BackoffSeconds").Get<int[]>() ?? new[] { 1, 2, 4 };
            _backoffSeconds = backoffConfig.Length > 0 ? backoffConfig : new[] { 1, 2, 4 };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = configuration["Kafka:GroupId"] ?? "retry-service-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                SessionTimeoutMs = 30000
            };

            _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Retry Consumer Service started.");
            _consumer.Subscribe(_failedTopic);
            _logger.LogInformation("Subscribed to Kafka topic: {Topic}", _failedTopic);

            var consumeTask = ConsumeFailedEventsAsync(stoppingToken);
            var retryTask = RetryPendingEventsAsync(stoppingToken);

            try
            {
                await Task.WhenAll(consumeTask, retryTask);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Retry Consumer Service is stopping.");
            }
            finally
            {
                _consumer.Close();
                _consumer.Dispose();
                _producer.Flush(TimeSpan.FromSeconds(5));
                _producer.Dispose();
            }
        }

        private async Task ConsumeFailedEventsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    if (consumeResult == null)
                    {
                        continue;
                    }

                    var messageValue = consumeResult.Message.Value;
                    _logger.LogInformation("Received failed event from Kafka: {Message}", messageValue);
                    await RegisterFailedEventAsync(messageValue);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consumption error: {Reason}", ex.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error consuming message from Kafka.");
                }
            }
        }

        private async Task RetryPendingEventsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var pendingCount = await _retryRepository.GetPendingRetryCountAsync();
                    _alertManager.SetPendingRetries(pendingCount);

                    var readyStates = await _retryRepository.GetReadyForRetryAsync();
                    foreach (var state in readyStates)
                    {
                        if (stoppingToken.IsCancellationRequested)
                        {
                            break;
                        }

                        await ProcessRetryAsync(state, stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Retry loop failed.");
                }

                await Task.Delay(_pollIntervalMs, stoppingToken);
            }
        }

        private async Task RegisterFailedEventAsync(string messageValue)
        {
            try
            {
                using var document = JsonDocument.Parse(messageValue);
                var root = document.RootElement;

                var commandId = ExtractCommandId(root);
                if (string.IsNullOrWhiteSpace(commandId))
                {
                    commandId = CreateFallbackCommandId(messageValue);
                }

                var existingState = await _retryRepository.GetRetryStateAsync(commandId);
                if (existingState?.Status is "completed" or "dead_letter")
                {
                    _logger.LogInformation("Skipping failed event because CommandId {CommandId} is already terminal with status {Status}.", commandId, existingState.Status);
                    return;
                }

                var errorType = root.TryGetProperty("errorType", out var etProp) ? etProp.GetString() : "Unknown";
                var errorMessage = root.TryGetProperty("errorMessage", out var emProp) ? emProp.GetString() : string.Empty;
                var nextRetryTime = DateTime.UtcNow.AddSeconds(GetBackoffSeconds(1));

                var state = new RetryState
                {
                    CommandId = commandId,
                    AttemptCount = existingState?.AttemptCount ?? 0,
                    NextRetryTime = nextRetryTime,
                    LastError = $"{errorType}: {errorMessage}",
                    FailedEventJson = messageValue,
                    Status = "pending",
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                };

                var saved = await _retryRepository.UpsertPendingRetryAsync(state);
                if (saved)
                {
                    _logger.LogInformation("Registered retry state for CommandId={CommandId} with next retry at {NextRetryTime}.", commandId, nextRetryTime);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Failed to parse failed event JSON: {Message}", messageValue);
            }
        }

        private async Task ProcessRetryAsync(RetryState state, CancellationToken stoppingToken)
        {
            if (_circuitOpenUntil.HasValue && DateTimeOffset.UtcNow < _circuitOpenUntil.Value)
            {
                _logger.LogWarning("Circuit breaker open until {OpenUntil}. Skipping retry for CommandId={CommandId}.", _circuitOpenUntil.Value, state.CommandId);
                return;
            }

            using var document = JsonDocument.Parse(state.FailedEventJson ?? string.Empty);
            var root = document.RootElement;
            var commandId = state.CommandId ?? ExtractCommandId(root) ?? CreateFallbackCommandId(state.FailedEventJson ?? string.Empty);
            var sourceTopic = root.TryGetProperty("sourceTopic", out var sourceProp) ? sourceProp.GetString() : _defaultRetryTopic;
            var originalPayload = ExtractOriginalPayload(root);

            if (string.IsNullOrWhiteSpace(originalPayload))
            {
                var deadLetterReason = "Missing original payload in failed event.";
                await SendToDeadLetterAsync(commandId, state.FailedEventJson ?? string.Empty, deadLetterReason, state.AttemptCount, sourceTopic);
                return;
            }

            var nextAttempt = state.AttemptCount + 1;
            var retryReason = $"Retry attempt {nextAttempt} failed: downstream processing requires another try.";

            if (nextAttempt >= _maxRetries)
            {
                await SendToDeadLetterAsync(commandId, state.FailedEventJson ?? string.Empty, retryReason, nextAttempt, sourceTopic);
                return;
            }

            try
            {
                await _producer.ProduceAsync(sourceTopic, new Message<string, string>
                {
                    Key = commandId,
                    Value = originalPayload
                });

                var backoffSeconds = GetBackoffSeconds(nextAttempt);
                var nextRetryTime = DateTime.UtcNow.AddSeconds(backoffSeconds);
                var successReason = $"Retry attempt {nextAttempt} succeeded downstream.";
                var updatedState = new RetryState
                {
                    CommandId = commandId,
                    AttemptCount = nextAttempt,
                    NextRetryTime = nextRetryTime,
                    LastError = successReason,
                    FailedEventJson = state.FailedEventJson,
                    Status = "pending",
                    CreatedAt = state.CreatedAt,
                    UpdatedAt = DateTime.UtcNow
                };

                await _retryRepository.UpdateRetryAttemptAsync(updatedState);
                _alertManager.IncrementRetryAttempt(nextAttempt);
                _consecutiveDownstreamFailures = 0;
                _logger.LogInformation("Retry succeeded for CommandId={CommandId}. Republished to topic {Topic} and scheduled next attempt if needed.", commandId, sourceTopic);
            }
            catch (Exception ex)
            {
                _consecutiveDownstreamFailures++;
                var failureReason = $"Retry attempt {nextAttempt} failed: {ex.Message}";

                if (_consecutiveDownstreamFailures >= _circuitBreakerThreshold)
                {
                    _circuitOpenUntil = DateTimeOffset.UtcNow.AddSeconds(_circuitBreakerCooldownSeconds);
                    _consecutiveDownstreamFailures = 0;
                    await _alertManager.SendCircuitBreakerAlertAsync($"Circuit opened for {_circuitBreakerCooldownSeconds} seconds after repeated downstream failures.");
                    _logger.LogWarning("Circuit breaker opened for {CooldownSeconds} seconds.", _circuitBreakerCooldownSeconds);
                }

                if (nextAttempt >= _maxRetries)
                {
                    await SendToDeadLetterAsync(commandId, state.FailedEventJson ?? string.Empty, failureReason, nextAttempt, sourceTopic);
                    return;
                }

                var backoffSeconds = GetBackoffSeconds(nextAttempt);
                var nextRetryTime = DateTime.UtcNow.AddSeconds(backoffSeconds);
                var updatedState = new RetryState
                {
                    CommandId = commandId,
                    AttemptCount = nextAttempt,
                    NextRetryTime = nextRetryTime,
                    LastError = failureReason,
                    FailedEventJson = state.FailedEventJson,
                    Status = "pending",
                    CreatedAt = state.CreatedAt,
                    UpdatedAt = DateTime.UtcNow
                };

                await _retryRepository.UpdateRetryAttemptAsync(updatedState);
                _alertManager.IncrementRetryAttempt(nextAttempt);
                _logger.LogWarning("Retry scheduled for CommandId={CommandId}. Attempt={Attempt}/{Max}, NextRetryTime={NextRetryTime}.", commandId, nextAttempt, _maxRetries, nextRetryTime);
            }
        }

        private async Task SendToDeadLetterAsync(string commandId, string failedEventJson, string reason, int attemptCount, string? sourceTopic)
        {
            _logger.LogError("Max retries exceeded for CommandId={CommandId}. Sending to dead letter queue.", commandId);
            await _deadLetterService.PublishDeadLetterAsync(commandId, failedEventJson, reason, attemptCount, sourceTopic);
            await _retryRepository.MarkAsDeadLetterAsync(commandId, reason);
            await _alertManager.SendDeadLetterAlertAsync(commandId, reason);
        }

        private int GetBackoffSeconds(int attemptNumber)
        {
            if (attemptNumber <= 1)
            {
                return _backoffSeconds[0];
            }

            var index = Math.Min(attemptNumber - 1, _backoffSeconds.Length - 1);
            return _backoffSeconds[index];
        }

        private static string? ExtractCommandId(JsonElement root)
        {
            if (root.TryGetProperty("command_id", out var commandIdProp) && commandIdProp.ValueKind == JsonValueKind.String)
            {
                return commandIdProp.GetString();
            }

            if (root.TryGetProperty("commandId", out var commandIdPascalProp) && commandIdPascalProp.ValueKind == JsonValueKind.String)
            {
                return commandIdPascalProp.GetString();
            }

            if (root.TryGetProperty("failedId", out var failedIdProp) && failedIdProp.ValueKind == JsonValueKind.String)
            {
                return failedIdProp.GetString();
            }

            return null;
        }

        private static string? ExtractOriginalPayload(JsonElement root)
        {
            if (root.TryGetProperty("rawEvent", out var rawEventProp) && rawEventProp.ValueKind == JsonValueKind.String)
            {
                return rawEventProp.GetString();
            }

            if (root.TryGetProperty("originalPayload", out var payloadProp) && payloadProp.ValueKind == JsonValueKind.String)
            {
                return payloadProp.GetString();
            }

            return null;
        }

        private static string CreateFallbackCommandId(string rawValue)
        {
            var bytes = Encoding.UTF8.GetBytes(rawValue ?? string.Empty);
            return Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        }
    }
}
