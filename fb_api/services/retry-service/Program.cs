using Prometheus;
using RetryService.Data;
using RetryService.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddLogging(config =>
{
    config.AddConsole();
});

builder.Services.AddSingleton<RetryRepository>();
builder.Services.AddSingleton<DeadLetterService>();
builder.Services.AddSingleton<AlertManager>();
builder.Services.AddHostedService<RetryConsumerService>();

var host = builder.Build();

var metricsPort = host.Services.GetRequiredService<IConfiguration>()
    .GetValue<int?>("Alerts:PrometheusMetricsPort") ?? 19090;
var enableMetrics = host.Services.GetRequiredService<IConfiguration>()
    .GetValue<bool>("Alerts:EnablePrometheusMetrics", false);

var logger = host.Services.GetRequiredService<ILogger<Program>>();

if (enableMetrics)
{
    try
    {
        var metricsServer = new MetricServer(port: metricsPort);
        metricsServer.Start();
        logger.LogInformation("Prometheus metrics server started on port {MetricsPort}", metricsPort);
    }
    catch (Exception ex)
    {
        logger.LogWarning(ex, "Failed to start Prometheus metrics server on port {Port}. Continuing without metrics.", metricsPort);
    }
}
else
{
    logger.LogInformation("Prometheus metrics server is disabled by configuration.");
}

host.Run();
