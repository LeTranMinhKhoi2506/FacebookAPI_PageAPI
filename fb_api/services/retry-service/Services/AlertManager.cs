using System.Net.Http.Json;
using Prometheus;

namespace RetryService.Services
{
    public class AlertManager
    {
        private readonly ILogger<AlertManager> _logger;
        private readonly IConfiguration _configuration;
        private readonly HttpClient _httpClient;

        private static readonly Counter DeadLetterCounter = Metrics.CreateCounter(
            "retry_service_dead_letter_total",
            "Total messages sent to dead letter queue",
            new CounterConfiguration { LabelNames = new[] { "reason" } }
        );

        private static readonly Gauge RetryStateGauge = Metrics.CreateGauge(
            "retry_service_pending_retries",
            "Current number of pending retries"
        );

        private static readonly Counter RetryAttemptCounter = Metrics.CreateCounter(
            "retry_service_retry_attempts_total",
            "Total retry attempts",
            new CounterConfiguration { LabelNames = new[] { "attempt_number" } }
        );

        private static readonly Counter CircuitBreakerCounter = Metrics.CreateCounter(
            "retry_service_circuit_breaker_open_total",
            "Total circuit breaker open events"
        );

        public AlertManager(IConfiguration configuration, ILogger<AlertManager> logger)
        {
            _configuration = configuration;
            _logger = logger;
            _httpClient = new HttpClient();
        }

        public async Task SendDeadLetterAlertAsync(string commandId, string reason)
        {
            try
            {
                var slackEnabled = _configuration.GetValue<bool>("Alerts:SlackEnabled", false);
                DeadLetterCounter.Labels(reason).Inc();
                _logger.LogWarning("Dead letter alert recorded. CommandId={CommandId}, Reason={Reason}", commandId, reason);

                if (slackEnabled)
                {
                    await SendSlackNotificationAsync("Dead Letter Queue Alert", commandId, reason);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending dead letter alert.");
            }
        }

        public async Task SendCircuitBreakerAlertAsync(string reason)
        {
            try
            {
                CircuitBreakerCounter.Inc();
                _logger.LogWarning("Circuit breaker alert recorded. Reason={Reason}", reason);

                if (_configuration.GetValue<bool>("Alerts:SlackEnabled", false))
                {
                    await SendSlackNotificationAsync("Circuit Breaker Open", "n/a", reason);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending circuit breaker alert.");
            }
        }

        public async Task SendSlackNotificationAsync(string title, string commandId, string reason)
        {
            try
            {
                var webhookUrl = _configuration["Alerts:SlackWebhookUrl"];
                if (string.IsNullOrWhiteSpace(webhookUrl))
                {
                    _logger.LogWarning("Slack webhook URL not configured. Skipping Slack notification.");
                    return;
                }

                var payload = new
                {
                    text = $"🚨 {title}",
                    blocks = new[]
                    {
                        new
                        {
                            type = "section",
                            text = new
                            {
                                type = "mrkdwn",
                                text = $"*{title}*\n\n*Command ID:* {commandId}\n*Reason:* {reason}\n*Time:* {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC"
                            }
                        }
                    }
                };

                var response = await _httpClient.PostAsJsonAsync(webhookUrl, payload);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("Slack notification sent successfully for CommandId: {CommandId}", commandId);
                }
                else
                {
                    _logger.LogWarning("Slack notification failed with status code: {StatusCode}", response.StatusCode);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending Slack notification for CommandId: {CommandId}", commandId);
            }
        }

        public void IncrementRetryAttempt(int attemptNumber)
        {
            try
            {
                RetryAttemptCounter.Labels(attemptNumber.ToString()).Inc();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error incrementing retry attempt metric.");
            }
        }

        public void SetPendingRetries(int count)
        {
            try
            {
                RetryStateGauge.Set(count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error setting pending retries metric.");
            }
        }
    }
}
