using Confluent.Kafka;
using System.Text.Json;
using FacebookAPI___PageAPI.Data;
using FacebookAPI___PageAPI.models;
using FacebookAPI___PageAPI.services;

namespace FacebookAPI___PageAPI.Services
{
    public class KafkaReplyConsumerWorker : BackgroundService
    {
        private readonly ILogger<KafkaReplyConsumerWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly BackendStateRepository _backendStateRepository;
        private readonly IConsumer<Ignore, string>? _consumer;
        private readonly string _topic;
        private readonly bool _isEnabled;

        public KafkaReplyConsumerWorker(
            ILogger<KafkaReplyConsumerWorker> logger,
            IConfiguration configuration,
            IServiceProvider serviceProvider,
            BackendStateRepository backendStateRepository)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _backendStateRepository = backendStateRepository;

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            _topic = configuration["Kafka:ReplyCommandsTopic"] ?? "reply_commands";
            var groupId = configuration["Kafka:GroupId"] ?? "backend-api-reply-group";

            // Support turning off Kafka consumer if needed (e.g. during specific test runs)
            _isEnabled = configuration.GetValue<bool>("Kafka:EnableConsumer", true);

            if (_isEnabled)
            {
                var consumerConfig = new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = groupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnableAutoCommit = true
                };

                try
                {
                    _consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
                    _logger.LogInformation("KafkaReplyConsumerWorker initialized for topic '{Topic}' on broker '{Servers}'", _topic, bootstrapServers);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to initialize Kafka consumer. Background processing will be unavailable.");
                }
            }
            else
            {
                _logger.LogInformation("KafkaReplyConsumerWorker is disabled by configuration.");
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!_isEnabled || _consumer == null)
            {
                _logger.LogInformation("KafkaReplyConsumerWorker is disabled or consumer is not initialized. Skipping background processing.");
                return;
            }

            _logger.LogInformation("KafkaReplyConsumerWorker started.");
            _consumer.Subscribe(_topic);
            _logger.LogInformation("Subscribed to Kafka topic '{Topic}'. Waiting for messages...", _topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(stoppingToken);

                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            continue;
                        }

                        var messageValue = consumeResult.Message.Value;
                        _logger.LogInformation("----------------------------------------");
                        _logger.LogInformation("Received reply command from Kafka key: {Key}", consumeResult.Message.Key);
                        _logger.LogInformation("Payload: {MessageValue}", messageValue);

                        await ProcessCommandAsync(messageValue);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consumption error: {Reason}", ex.Error.Reason);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error occurred while processing message: {Message}", ex.Message);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("KafkaReplyConsumerWorker is stopping due to cancellation request.");
            }
            finally
            {
                _consumer.Close();
                _consumer.Dispose();
                _logger.LogInformation("KafkaReplyConsumerWorker connection closed.");
            }
        }

        private async Task ProcessCommandAsync(string messageValue)
        {
            try
            {
                using var doc = JsonDocument.Parse(messageValue);
                var root = doc.RootElement;

                // 1. Extract command properties
                var commandType = root.TryGetProperty("commandType", out var cmdProp) ? cmdProp.GetString() : null;
                var messageText = root.TryGetProperty("message", out var msgProp) ? msgProp.GetString() : null;
                var eventId = root.TryGetProperty("eventId", out var idProp) ? idProp.GetString() : null;

                if (string.IsNullOrWhiteSpace(commandType))
                {
                    _logger.LogWarning("Skipping command: Missing 'commandType' property.");
                    return;
                }

                _logger.LogInformation("Processing Command: EventId={EventId}, CommandType={CommandType}, Message={Message}", eventId, commandType, messageText);

                var auditCommandId = eventId ?? Guid.NewGuid().ToString();
                await _backendStateRepository.SaveAuditAsync(new CommentAuditRecord
                {
                    CommandId = auditCommandId,
                    CommentId = root.TryGetProperty("comment_id", out var cId) ? cId.GetString() : null,
                    CommandType = commandType,
                    Message = messageText,
                    Status = "received",
                    CreatedAt = DateTime.UtcNow,
                    UpdatedAt = DateTime.UtcNow
                });

                // 2. Extract comment_id
                string? commentId = null;

                if (root.TryGetProperty("originalEvent", out var origProp))
                {
                    if (origProp.ValueKind == JsonValueKind.String)
                    {
                        var origString = origProp.GetString();
                        if (!string.IsNullOrWhiteSpace(origString))
                        {
                            commentId = ExtractCommentIdFromRawEvent(origString);
                        }
                    }
                    else if (origProp.ValueKind == JsonValueKind.Object)
                    {
                        commentId = ExtractCommentIdFromElement(origProp);
                    }
                }

                // Fallback: check top-level properties if comment_id is directly present
                if (string.IsNullOrWhiteSpace(commentId) && root.TryGetProperty("comment_id", out var directCommentProp))
                {
                    commentId = directCommentProp.GetString();
                }

                if (string.IsNullOrWhiteSpace(commentId))
                {
                    _logger.LogError("Could not extract a valid 'comment_id' from the command. Original event: {OrigEvent}", 
                        root.TryGetProperty("originalEvent", out var o) ? o.ToString() : "none");
                    return;
                }

                _logger.LogInformation("Extracted Comment ID: {CommentId}", commentId);

                // 3. Create scope and resolve FacebookService to execute the API call
                using var scope = _serviceProvider.CreateScope();
                var facebookService = scope.ServiceProvider.GetRequiredService<FacebookService>();

                // 4. Dispatch action based on commandType
                switch (commandType.ToLowerInvariant())
                {
                    case "auto_reply":
                    case "needs_manual_review":
                        if (string.IsNullOrWhiteSpace(messageText))
                        {
                            _logger.LogWarning("Command type is '{CommandType}' but 'message' is empty. Skipping reply.", commandType);
                            await _backendStateRepository.SaveAuditAsync(new CommentAuditRecord
                            {
                                CommandId = eventId ?? Guid.NewGuid().ToString(),
                                CommentId = commentId,
                                CommandType = commandType,
                                Message = messageText,
                                Status = "skipped",
                                Error = "Missing message",
                                CreatedAt = DateTime.UtcNow,
                                UpdatedAt = DateTime.UtcNow
                            });
                        }
                        else
                        {
                            var replyResult = await facebookService.ReplyToCommentAsync(commentId, messageText);
                            _logger.LogInformation("Successfully replied to comment {CommentId}. Response: {Response}", commentId, replyResult);
                            await _backendStateRepository.SaveAuditAsync(new CommentAuditRecord
                            {
                                CommandId = eventId ?? Guid.NewGuid().ToString(),
                                CommentId = commentId,
                                CommandType = commandType,
                                Message = messageText,
                                Status = "completed",
                                CreatedAt = DateTime.UtcNow,
                                UpdatedAt = DateTime.UtcNow
                            });
                        }
                        break;

                    case "hide_comment":
                        var hideResult = await facebookService.HideCommentAsync(commentId);
                        _logger.LogInformation("Successfully hid comment {CommentId}. Response: {Response}", commentId, hideResult);
                        await _backendStateRepository.SaveAuditAsync(new CommentAuditRecord
                        {
                            CommandId = eventId ?? Guid.NewGuid().ToString(),
                            CommentId = commentId,
                            CommandType = commandType,
                            Message = messageText,
                            Status = "completed",
                            CreatedAt = DateTime.UtcNow,
                            UpdatedAt = DateTime.UtcNow
                        });
                        break;

                    default:
                        _logger.LogWarning("Unhandled commandType: '{CommandType}' for comment {CommentId}", commandType, commentId);
                        await _backendStateRepository.SaveAuditAsync(new CommentAuditRecord
                        {
                            CommandId = eventId ?? Guid.NewGuid().ToString(),
                            CommentId = commentId,
                            CommandType = commandType,
                            Message = messageText,
                            Status = "ignored",
                            Error = "Unhandled command type",
                            CreatedAt = DateTime.UtcNow,
                            UpdatedAt = DateTime.UtcNow
                        });
                        break;
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Failed to parse command JSON payload: {Payload}", messageValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing command: {Message}", ex.Message);
            }
        }

        private string? ExtractCommentIdFromRawEvent(string rawEventJson)
        {
            try
            {
                using var doc = JsonDocument.Parse(rawEventJson);
                return ExtractCommentIdFromElement(doc.RootElement);
            }
            catch
            {
                return null;
            }
        }

        private string? ExtractCommentIdFromElement(JsonElement element)
        {
            if (element.TryGetProperty("comment_id", out var commentProp) && commentProp.ValueKind == JsonValueKind.String)
            {
                return commentProp.GetString();
            }

            // Check inside raw_payload if needed
            if (element.TryGetProperty("raw_payload", out var payloadProp))
            {
                if (payloadProp.TryGetProperty("entry", out var entryProp) && entryProp.ValueKind == JsonValueKind.Array)
                {
                    foreach (var entry in entryProp.EnumerateArray())
                    {
                        if (entry.TryGetProperty("changes", out var changesProp) && changesProp.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var change in changesProp.EnumerateArray())
                            {
                                if (change.TryGetProperty("value", out var valProp) && valProp.TryGetProperty("comment_id", out var cProp))
                                {
                                    return cProp.GetString();
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }
    }
}
