using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Abstractions.Attributes;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Sample.SubscriberScan;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                services.AddRabbitMq(options =>
                {
                options.HostName = "localhost";
                options.UserName = "guest";
                options.Password = "guest";
                });
                
                // 注册消息处理器 (使用源码生成器自动注册)
                services.AddScoped<OrderMessageHandler>();
                services.AddScoped<UserEventHandler>();
                
                // 启动订阅宿主
                services.AddSingleton(sp =>
                {
                    var registrations = new List<SubscriberRegistration>
                    {
                        new()
                        {
                            Name = "OrderProcessor",
                            QueueName = "order.queue",
                            ConsumerType = typeof(OrderMessageHandler),
                            ConnectionName = "Default",
                            PrefetchCount = 10
                        },
                        new()
                        {
                            Name = "UserEventProcessor",
                            QueueName = "user.events",
                            ConsumerType = typeof(UserEventHandler),
                            ConnectionName = "Default",
                            PrefetchCount = 20
                        }
                    };
                    return new RabbitMqSubscriberHost(sp, registrations);
                });
                
                services.AddHostedService<SubscriberHostService>();
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .Build();

        await host.RunAsync();
    }
}

/// <summary>
/// 订单消息处理器 - 使用特性标注
/// </summary>
[RabbitMqConsumer(Queue = "order.queue", PrefetchCount = 10)]
public class OrderMessageHandler : IMessageHandler
{
    private readonly ILogger<OrderMessageHandler> _logger;

    public OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
            _logger.LogInformation("OrderMessageHandler: Processing order - {Message}", message);

            // 模拟处理
            await Task.Delay(100, cancellationToken);

            _logger.LogInformation("OrderMessageHandler: Order processed successfully");
            return HandleResult.SuccessResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OrderMessageHandler: Failed to process order");
            return HandleResult.Retry(ex.Message);
        }
    }
}

/// <summary>
/// 用户事件处理器 - 使用特性标注
/// </summary>
[RabbitMqConsumer(Queue = "user.events", PrefetchCount = 20)]
public class UserEventHandler : IMessageHandler
{
    private readonly ILogger<UserEventHandler> _logger;

    public UserEventHandler(ILogger<UserEventHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
            _logger.LogInformation("UserEventHandler: Processing user event - {Message}", message);

            // 模拟处理
            await Task.Delay(50, cancellationToken);

            _logger.LogInformation("UserEventHandler: User event processed successfully");
            return HandleResult.SuccessResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "UserEventHandler: Failed to process user event");
            return HandleResult.Reject(ex.Message);
        }
    }
}

/// <summary>
/// 订阅宿主后台服务
/// </summary>
public class SubscriberHostService : BackgroundService
{
    private readonly RabbitMqSubscriberHost _subscriberHost;
    private readonly ILogger<SubscriberHostService> _logger;

    public SubscriberHostService(
        RabbitMqSubscriberHost subscriberHost,
        ILogger<SubscriberHostService> logger)
    {
        _subscriberHost = subscriberHost;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("SubscriberHostService: Starting subscriber host...");
        
        try
        {
            await _subscriberHost.StartAsync(stoppingToken);
            _logger.LogInformation("SubscriberHostService: Subscriber host started successfully");
            
            // 保持运行
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("SubscriberHostService: Subscriber host stopping...");
        }
        finally
        {
            await _subscriberHost.StopAsync(stoppingToken);
            _logger.LogInformation("SubscriberHostService: Subscriber host stopped");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("SubscriberHostService: Stopping...");
        await _subscriberHost.StopAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }
}
