using CoreService;
using Microsoft.SemanticKernel;
#pragma warning disable SKEXP0070

var builder = Host.CreateApplicationBuilder(args);

// Ensure your Gemini API Key is configured in appsettings.json or via environment variables
var geminiApiKey = builder.Configuration["AI:Gemini:ApiKey"] ?? "your-gemini-api-key";

// Use Semantic Kernel with Google Gemini
builder.Services.AddKernel()
    .AddGoogleAIGeminiChatCompletion("gemini-2.5-flash", geminiApiKey);

builder.Services.AddHostedService<KafkaConsumerService>();

var host = builder.Build();
host.Run();

