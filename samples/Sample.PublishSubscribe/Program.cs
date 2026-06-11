using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Sample.PublishSubscribe;

public class OrderNotificationHandler : IMessageHandler
{
    private readonly ILogger<OrderNotificationHandler> _logger;

    public OrderNotificationHandler(ILogger<OrderNotificationHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[订单通知] {Message}", message);
        await Task.Delay(50, cancellationToken);
        return HandleResult.SuccessResult;
    }
}

public class InventoryAlertHandler : IMessageHandler
{
    private readonly ILogger<InventoryAlertHandler> _logger;

    public InventoryAlertHandler(ILogger<InventoryAlertHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[库存警告] {Message}", message);
        await Task.Delay(50, cancellationToken);
        return HandleResult.SuccessResult;
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== RabbitMQ 发布订阅示例 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<OrderNotificationHandler>();
        builder.Services.AddScoped<InventoryAlertHandler>();

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "OrderNotificationSubscriber",
            QueueName = "order.notification.queue",
            ExchangeName = "orders.fanout",
            RoutingKey = string.Empty,
            ConsumerType = typeof(OrderNotificationHandler),
            PrefetchCount = 10
        });

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "InventoryAlertSubscriber",
            QueueName = "inventory.alert.queue",
            ExchangeName = "orders.fanout",
            RoutingKey = string.Empty,
            ConsumerType = typeof(InventoryAlertHandler),
            PrefetchCount = 10
        });

        builder.Services.AddHostedService<Hyz.RabbitMQ.Subscriber.RabbitMqSubscriberHost>();

        var host = builder.Build();

        Console.WriteLine("启动发布订阅示例...\n");

        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

            for (int i = 1; i <= 3; i++)
            {
                var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes($"新订单 #{i} - 金额: ¥{100 * i}"));
                await publisher.PublishToExchangeAsync("orders.fanout", string.Empty, message);
                Console.WriteLine($"已发布订单通知 #{i}");
            }

            Console.WriteLine("\n所有订阅者应该都收到相同的消息！\n");
        }

        Console.WriteLine("按 Ctrl+C 退出...\n");

        await host.RunAsync();
    }
}
