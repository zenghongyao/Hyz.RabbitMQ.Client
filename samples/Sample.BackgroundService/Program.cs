using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Sample.BackgroundService;

public class MetricsPublisherService : Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly IPublisherService _publisher;
    private readonly ILogger<MetricsPublisherService> _logger;
    private readonly Random _random = new();

    public MetricsPublisherService(
        IPublisherService publisher,
        ILogger<MetricsPublisherService> logger)
    {
        _publisher = publisher;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("指标发布服务已启动");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var metrics = new
                {
                    Timestamp = DateTime.UtcNow,
                    CpuUsage = _random.Next(10, 95),
                    MemoryUsage = _random.Next(30, 85)
                };

                var message = new MessageBody(
                    System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(metrics));

                await _publisher.PublishToExchangeAsync(
                    "system.metrics",
                    "metrics.cpu",
                    message,
                    new PublishOptions
                    {
                        ContentType = "application/json",
                        Persistent = true
                    });

                _logger.LogDebug("已发布指标: CPU={Cpu}%", metrics.CpuUsage);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布指标失败");
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }

        _logger.LogInformation("指标发布服务已停止");
    }
}

public class AlertHandler : IMessageHandler
{
    private readonly ILogger<AlertHandler> _logger;

    public AlertHandler(ILogger<AlertHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = System.Text.Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogWarning("收到告警: {Message}", content);
        await Task.Delay(100, cancellationToken);
        return HandleResult.SuccessResult;
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== RabbitMQ 后台服务示例 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<AlertHandler>();

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "AlertSubscriber",
            QueueName = "system.alerts",
            ExchangeName = "system.metrics",
            RoutingKey = "metrics.#",
            ConsumerType = typeof(AlertHandler),
            PrefetchCount = 10
        });

        builder.Services.AddHostedService<MetricsPublisherService>();
        builder.Services.AddHostedService<RabbitMqSubscriberHost>();

        var host = builder.Build();

        Console.WriteLine("启动后台服务示例...\n");
        Console.WriteLine("- 指标发布服务: 每5秒发布一次系统指标");
        Console.WriteLine("- 告警订阅服务: 订阅所有以 'metrics.' 开头的消息\n");

        await host.RunAsync();
    }
}
