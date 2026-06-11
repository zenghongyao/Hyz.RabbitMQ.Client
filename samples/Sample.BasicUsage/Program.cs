using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Sample.BasicUsage;

public class OrderCreatedHandler : IMessageHandler
{
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
            _logger.LogInformation("收到订单消息: {Message}", message);
            await Task.Delay(100, cancellationToken);
            return HandleResult.SuccessResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "处理订单消息失败");
            return HandleResult.Retry(ex.Message);
        }
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Hyz.RabbitMQ 示例程序 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<OrderCreatedHandler>();

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "OrderConsumer",
            QueueName = "order.created",
            ConsumerType = typeof(OrderCreatedHandler),
            PrefetchCount = 10
        });

        builder.Services.AddHostedService<RabbitMqSubscriberHost>();

        var host = builder.Build();

        using (var scope = host.Services.CreateScope())
        {
            var connectionManager = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            Console.WriteLine($"已注册的连接: {string.Join(", ", connectionManager.GetAllConnectionNames())}");
            Console.WriteLine($"默认连接: {connectionManager.Default.Name}");
        }

        Console.WriteLine("\n按 Ctrl+C 退出...\n");

        await host.RunAsync();
    }
}
