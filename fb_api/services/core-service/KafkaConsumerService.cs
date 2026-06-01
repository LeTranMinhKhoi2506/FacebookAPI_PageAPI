using Confluent.Kafka;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CoreService.Models;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.Data.Sqlite;
using System.Globalization;
using System.Collections.Concurrent;
using System.Net.Http;
using System.IO;
using System.Threading;

namespace CoreService;

public class KafkaConsumerService : BackgroundService
{
    private readonly ILogger<KafkaConsumerService> _logger;

        private readonly IConsumer<Ignore, string> _consumer;
        private readonly string _topic;
        private readonly IProducer<string, string> _producer;
            private readonly string _failedTopic;
            private readonly string _replyTopic;
            private readonly IChatCompletionService _chatService;
            private readonly string _auditConnectionString;
            private readonly int _rateLimitWindowMinutes;
            private readonly int _rateLimitMaxCount;
            private readonly string _aiFallbackIntent = "tương tác";
            private readonly string _aiFallbackSentiment = "trung tính";

        public KafkaConsumerService(
            ILogger<KafkaConsumerService> logger,
            IConfiguration configuration,
            IChatCompletionService chatService)
        {
            _logger = logger;
            _chatService = chatService;

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
            _topic = configuration["Kafka:Topic"] ?? "raw_events";
            _failedTopic = configuration["Kafka:FailedTopic"] ?? "send_failed";
            _replyTopic = configuration["Kafka:ReplyCommandsTopic"] ?? "reply_commands";
            _auditConnectionString = configuration["Database:ConnectionString"] ?? "Data Source=core_audit.db";
            _rateLimitWindowMinutes = configuration.GetValue<int>("RateLimit:WindowMinutes", 1);
            _rateLimitMaxCount = configuration.GetValue<int>("RateLimit:MaxCount", 20);
            InitializeAuditDatabase();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = configuration["Kafka:GroupId"] ?? "core-service-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
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
            _logger.LogInformation("Kafka Consumer Service started.");

            _consumer.Subscribe(_topic);

            _logger.LogInformation("Subscribed to Kafka topic: {Topic}", _topic);

            try
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

                        _logger.LogInformation("=======================================");
                        _logger.LogInformation("Received raw event from Kafka:");
                        _logger.LogInformation("{MessageValue}", messageValue);

                        await ProcessEventAsync(messageValue);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming message: {Reason}", ex.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka Consumer Service is stopping.");
            }
            finally
            {
                _consumer.Close();
                _consumer.Dispose();

                _producer.Flush(TimeSpan.FromSeconds(5));
                _producer.Dispose();
            }
        }

        private async Task ProcessEventAsync(string rawEvent)
        {
            try
            {
                _logger.LogInformation("Event status: received");

                // --- Loop Guard ---
                // Skip any event where the Page itself is the comment sender.
                // This prevents an infinite reply loop:
                //   KafkaReplyConsumerWorker replies → Facebook fires a Webhook event
                //   → core-service would reply again → repeat forever.
                if (IsPageBotReply(rawEvent))
                {
                    _logger.LogInformation("[LOOP GUARD] Event is a bot-generated comment (sender_id == page_id). Skipping to prevent infinite reply loop.");
                    await SaveAuditAsync(rawEvent, null, null, null, "loop_guard", "Bot reply detected", "skipped");
                    _logger.LogInformation("Event status: skipped_bot_reply");
                    return;
                }

                if (await IsRateLimitedAsync(rawEvent))
                {
                    _logger.LogWarning("Rate limit exceeded for event; skipping automated action.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "rate_limited", ExtractMessage(rawEvent), null, "rate_limit_exceeded", "rate_limited");
                    return;
                }

                var messageText = ExtractMessage(rawEvent);

                _logger.LogInformation("Extracted message: {MessageText}", messageText);

                if (IsSpam(messageText))
                {
                    _logger.LogWarning("Spam detected: {MessageText}", messageText);
                    _logger.LogInformation("Auto-decision: Ẩn bình luận hoặc chuyển sang hàng chờ kiểm duyệt.");
                    await PublishReplyCommandAsync(rawEvent, "hide_comment", "Spam comment detected (rule-based).");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "hide_comment", messageText, "spam", "rule_based_spam", "processed_spam");
                    _logger.LogInformation("Event status: processed_spam");
                    return;
                }

                if (rawEvent.Contains("FORCE_ERROR"))
                {
                    throw new Exception("Simulated processing error for testing retry flow.");
                }

                var analyzedData = await AnalyzeWithAIAsync(messageText);

                _logger.LogInformation(
                    "AI Analysis Result: Intent={Intent}, Sentiment={Sentiment}",
                    analyzedData.Intent,
                    analyzedData.Sentiment
                );

                if (IsLongWaitComplaint(messageText))
                {
                    _logger.LogInformation("Auto-decision: Xin lỗi và xin kiểm tra vì comment thể hiện chờ đợi quá lâu.");
                    await PublishReplyCommandAsync(rawEvent, "auto_reply", "Xin lỗi bạn vì đã để bạn chờ lâu, mình xin kiểm tra ngay cho bạn.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "auto_reply", messageText, analyzedData.Intent, analyzedData.Sentiment, "processed");
                }
                else if (analyzedData.Intent == "khiếu nại" && analyzedData.Sentiment == "tiêu cực")
                {
                    _logger.LogWarning("Auto-decision: Cần phản hồi khẩn cấp hoặc tạo ticket hỗ trợ.");
                    await PublishReplyCommandAsync(rawEvent, "needs_manual_review", "Cần xử lý khiếu nại.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "needs_manual_review", messageText, analyzedData.Intent, analyzedData.Sentiment, "processed");
                }
                else if (analyzedData.Intent == "hỏi giá")
                {
                    _logger.LogInformation("Auto-decision: Tự động phản hồi thông tin giá.");
                    await PublishReplyCommandAsync(rawEvent, "auto_reply", "Giá sản phẩm là 100.000 VNĐ.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "auto_reply", messageText, analyzedData.Intent, analyzedData.Sentiment, "processed");
                }
                else if (analyzedData.Intent == "spam")
                {
                    _logger.LogInformation("Auto-decision: Ẩn bình luận, cân nhắc cho vào blacklist.");

                    await PublishReplyCommandAsync(rawEvent, "hide_comment", "Spam comment detected.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "hide_comment", messageText, analyzedData.Intent, analyzedData.Sentiment, "processed");
                }
                else if (analyzedData.Intent == "khen" || (analyzedData.Intent == "tương tác" && analyzedData.Sentiment == "tích cực"))
                {
                    _logger.LogInformation("Auto-decision: Ghi nhận tương tác tích cực, phản hồi cảm ơn.");
                    await PublishReplyCommandAsync(rawEvent, "auto_reply", "Cảm ơn bạn đã quan tâm.");
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "auto_reply", messageText, analyzedData.Intent, analyzedData.Sentiment, "processed");
                }
                else
                {
                    _logger.LogInformation("Auto-decision: Không cần phản hồi tự động cho comment này (Intent={Intent}, Sentiment={Sentiment}).",
                        analyzedData.Intent, analyzedData.Sentiment);
                    await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), "ignored", messageText, analyzedData.Intent, analyzedData.Sentiment, "ignored");
                }

                _logger.LogInformation("Event status: processed");
                _logger.LogInformation("Event processed successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing event. Should publish to send_failed topic for retry.");
                await PublishFailedEventAsync(rawEvent, ex);
                await SaveAuditAsync(rawEvent, ExtractCommentIdFromRawEvent(rawEvent), null, ExtractMessage(rawEvent), null, ex.Message, "failed");
                _logger.LogInformation("Event status: failed");
            }
        }

        private string ExtractMessage(string rawEvent)
        {
            try
            {
                using var document = JsonDocument.Parse(rawEvent);
                var root = document.RootElement;

                if (root.TryGetProperty("message", out var messageProperty))
                {
                    return messageProperty.GetString() ?? string.Empty;
                }

                return rawEvent;
            }
            catch
            {
                return rawEvent;
            }
        }

        private bool IsSpam(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                return false;
            }

            var lowerMessage = message.ToLower();

            if (lowerMessage.Contains("http://")) return true;
            if (lowerMessage.Contains("https://")) return true;
            if (lowerMessage.Contains("spam")) return true;
            if (lowerMessage.Contains("khuyến mãi sốc")) return true;

            return false;
        }

        private bool IsLongWaitComplaint(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
            {
                return false;
            }

            var lowerMessage = message.ToLower();
            return lowerMessage.Contains("chờ quá lâu")
                || lowerMessage.Contains("đợi quá lâu")
                || lowerMessage.Contains("chờ lâu")
                || lowerMessage.Contains("lâu quá")
                || lowerMessage.Contains("đợi lâu")
                || lowerMessage.Contains("mình chờ quá lâu");
        }

        /// <summary>
        /// Detects if the event was generated by the Page itself (i.e., a bot reply).
        /// Returns true when sender_id equals page_id, which means the Page replied to a comment
        /// and Facebook sent a webhook back — this would trigger an infinite reply loop if not filtered.
        /// </summary>
        private bool IsPageBotReply(string rawEvent)
        {
            try
            {
                using var document = JsonDocument.Parse(rawEvent);
                var root = document.RootElement;

                var senderId = root.TryGetProperty("sender_id", out var senderProp)
                    ? senderProp.GetString()
                    : null;

                var pageId = root.TryGetProperty("page_id", out var pageProp)
                    ? pageProp.GetString()
                    : null;

                if (string.IsNullOrEmpty(senderId) || string.IsNullOrEmpty(pageId))
                    return false;

                return senderId == pageId;
            }
            catch
            {
                return false;
            }
        }

    private async Task<(string Intent, string Sentiment)> AnalyzeWithAIAsync(string text)
    {
        try
        {
                var prompt = $@"
                Analyze this Facebook comment.

                Return EXACTLY in this format:

                Intent=<intent>
                Sentiment=<sentiment>

                Possible intents:
                - hỏi giá
                - khiếu nại
                - tương tác
                - spam
                - khen

                Possible sentiments:
                - tích cực
                - trung tính
                - tiêu cực

                Comment:
                {text}
                ";

            var reply = await _chatService.GetChatMessageContentAsync(prompt);

            var content = reply.Content?.Trim() ?? "";

            _logger.LogInformation("Raw AI Response: {Content}", content);

            var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);

            string intent = _aiFallbackIntent;
            string sentiment = _aiFallbackSentiment;

            foreach (var line in lines)
            {
                if (line.StartsWith("Intent=", StringComparison.OrdinalIgnoreCase))
                {
                    intent = line.Replace("Intent=", "", StringComparison.OrdinalIgnoreCase).Trim();
                }

                if (line.StartsWith("Sentiment=", StringComparison.OrdinalIgnoreCase))
                {
                    sentiment = line.Replace("Sentiment=", "", StringComparison.OrdinalIgnoreCase).Trim();
                }
            }

            return (intent, sentiment);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AI Analysis failed, using fallback.");

            return (_aiFallbackIntent, _aiFallbackSentiment);
        }
    }

    private async Task PublishReplyCommandAsync(string rawEvent, string commandType, string message)
    {
        var eventId = Guid.NewGuid().ToString();
        var commentId = ExtractCommentIdFromRawEvent(rawEvent);

        var replyEvent = new
        {
            eventId = eventId,
            commandType = commandType,
            message = message,
            comment_id = commentId,
            originalEvent = rawEvent,
            createdAt = DateTime.UtcNow
        };
        var replyJson = JsonSerializer.Serialize(replyEvent);
        await _producer.ProduceAsync(_replyTopic, new Message<string, string>
        {
            Key = eventId,
            Value = replyJson
        });
        _logger.LogInformation("Published reply command to Kafka topic: {ReplyTopic}", _replyTopic);
    }

    private string? ExtractCommentIdFromRawEvent(string rawEvent)
    {
        try
        {
            using var doc = JsonDocument.Parse(rawEvent);
            var root = doc.RootElement;

            if (root.TryGetProperty("comment_id", out var commentProp) && commentProp.ValueKind == JsonValueKind.String)
            {
                return commentProp.GetString();
            }

            if (root.TryGetProperty("raw_payload", out var payloadProp))
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
        catch
        {
            return null;
        }
    }

    private async Task PublishFailedEventAsync(string rawEvent, Exception exception)
        {
            var commandId = CreateCommandId(rawEvent);
            var failedEvent = new
            {
                failedId = Guid.NewGuid().ToString(),
                command_id = commandId,
                sourceTopic = _topic,
                errorType = exception.GetType().Name,
                errorMessage = exception.Message,
                failedAt = DateTime.UtcNow,
                retryCount = 0,
                status = "failed",
                rawEvent = rawEvent
            };

            var failedJson = JsonSerializer.Serialize(failedEvent);

            await _producer.ProduceAsync(_failedTopic, new Message<string, string>
            {
                Key = failedEvent.failedId,
                Value = failedJson
            });

            _logger.LogWarning("Published failed event to Kafka topic: {FailedTopic}", _failedTopic);
            _logger.LogWarning("Failed event payload: {FailedJson}", failedJson);
        }

        private async Task SaveAuditAsync(string rawEvent, string? commentId, string? commandType, string? message, string? intent, string? sentimentOrError, string status)
        {
            try
            {
                using var connection = new SqliteConnection(_auditConnectionString);
                await connection.OpenAsync();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    INSERT INTO CommentAuditLog (CommandId, CommentId, CommandType, Message, Sentiment, Intent, Status, Error, CreatedAt, UpdatedAt)
                    VALUES ($CommandId, $CommentId, $CommandType, $Message, $Sentiment, $Intent, $Status, $Error, $CreatedAt, $UpdatedAt)
                    ON CONFLICT(CommandId) DO UPDATE SET
                        CommentId = excluded.CommentId,
                        CommandType = excluded.CommandType,
                        Message = excluded.Message,
                        Sentiment = excluded.Sentiment,
                        Intent = excluded.Intent,
                        Status = excluded.Status,
                        Error = excluded.Error,
                        UpdatedAt = excluded.UpdatedAt;";

                var commandId = CreateCommandId(rawEvent);
                command.Parameters.AddWithValue("$CommandId", commandId);
                command.Parameters.AddWithValue("$CommentId", (object?)commentId ?? DBNull.Value);
                command.Parameters.AddWithValue("$CommandType", (object?)commandType ?? DBNull.Value);
                command.Parameters.AddWithValue("$Message", (object?)message ?? DBNull.Value);
                command.Parameters.AddWithValue("$Sentiment", status == "failed" ? DBNull.Value : (object?)sentimentOrError ?? DBNull.Value);
                command.Parameters.AddWithValue("$Intent", (object?)intent ?? DBNull.Value);
                command.Parameters.AddWithValue("$Status", status);
                command.Parameters.AddWithValue("$Error", status == "failed" ? sentimentOrError : DBNull.Value);
                command.Parameters.AddWithValue("$CreatedAt", DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture));
                command.Parameters.AddWithValue("$UpdatedAt", DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture));

                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving core audit record.");
            }
        }

        private async Task<bool> IsRateLimitedAsync(string rawEvent)
        {
            var senderId = ExtractSenderId(rawEvent);
            if (string.IsNullOrWhiteSpace(senderId))
            {
                return false;
            }

            var windowStart = GetWindowStart(DateTime.UtcNow, _rateLimitWindowMinutes);
            using var connection = new SqliteConnection(_auditConnectionString);
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
                SELECT COALESCE(Count, 0)
                FROM CommentRateLimit
                WHERE SenderId = $SenderId AND WindowStart = $WindowStart
                LIMIT 1;";
            command.Parameters.AddWithValue("$SenderId", senderId);
            command.Parameters.AddWithValue("$WindowStart", windowStart.ToString("O", CultureInfo.InvariantCulture));

            var result = await command.ExecuteScalarAsync();
            var currentCount = Convert.ToInt32(result);
            if (currentCount >= _rateLimitMaxCount)
            {
                return true;
            }

            using var upsert = connection.CreateCommand();
            upsert.CommandText = @"
                INSERT INTO CommentRateLimit (SenderId, WindowStart, Count, UpdatedAt)
                VALUES ($SenderId, $WindowStart, $Count, datetime('now'))
                ON CONFLICT(SenderId, WindowStart) DO UPDATE SET
                    Count = excluded.Count,
                    UpdatedAt = datetime('now');";
            upsert.Parameters.AddWithValue("$SenderId", senderId);
            upsert.Parameters.AddWithValue("$WindowStart", windowStart.ToString("O", CultureInfo.InvariantCulture));
            upsert.Parameters.AddWithValue("$Count", currentCount + 1);
            await upsert.ExecuteNonQueryAsync();
            return false;
        }

        private void InitializeAuditDatabase()
        {
            try
            {
                using var connection = new SqliteConnection(_auditConnectionString);
                connection.Open();

                using var command = connection.CreateCommand();
                command.CommandText = @"
                    CREATE TABLE IF NOT EXISTS CommentAuditLog (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        CommandId TEXT NOT NULL UNIQUE,
                        CommentId TEXT,
                        CommandType TEXT NOT NULL,
                        Message TEXT,
                        Sentiment TEXT,
                        Intent TEXT,
                        Status TEXT NOT NULL,
                        Error TEXT,
                        CreatedAt TEXT NOT NULL DEFAULT (datetime('now')),
                        UpdatedAt TEXT NOT NULL DEFAULT (datetime('now'))
                    );

                    CREATE TABLE IF NOT EXISTS CommentRateLimit (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        SenderId TEXT NOT NULL,
                        WindowStart TEXT NOT NULL,
                        Count INTEGER NOT NULL DEFAULT 0,
                        UpdatedAt TEXT NOT NULL DEFAULT (datetime('now')),
                        UNIQUE(SenderId, WindowStart)
                    );";
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize core audit database.");
            }
        }

        private string? ExtractSenderId(string rawEvent)
        {
            try
            {
                using var document = JsonDocument.Parse(rawEvent);
                var root = document.RootElement;
                return root.TryGetProperty("sender_id", out var senderProp) ? senderProp.GetString() : null;
            }
            catch
            {
                return null;
            }
        }

        private DateTime GetWindowStart(DateTime utcNow, int windowMinutes)
        {
            var truncated = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, utcNow.Minute, 0, DateTimeKind.Utc);
            return truncated.AddMinutes(-(truncated.Minute % windowMinutes));
        }

        private string CreateCommandId(string rawEvent)
        {
            var bytes = Encoding.UTF8.GetBytes(rawEvent ?? string.Empty);
            var hash = SHA256.HashData(bytes);
            return Convert.ToHexString(hash).ToLowerInvariant();
        }
    }
