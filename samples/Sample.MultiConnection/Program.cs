using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Hyz.RabbitMQ 多连接示例 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        // 1. 注册默认连接
        builder.Services.AddRabbitMq("Default", options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
        });

        // 2. 注册订单系统专用连接
        builder.Services.AddRabbitMq("OrderSystem", options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.VirtualHost = "/orders";
            options.UserName = "guest";
            options.Password = "guest";
        });

        // 3. 注册支付系统专用连接
        builder.Services.AddRabbitMq("PaymentSystem", options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.VirtualHost = "/payments";
            options.UserName = "guest";
            options.Password = "guest";
        });

        var host = builder.Build();

        Console.WriteLine("多连接注册完成!\n");

        // 演示多连接发布
        using (var scope = host.Services.CreateScope())
        {
            var multiPublisher = scope.ServiceProvider.GetRequiredService<IMultiConnectionPublisherService>();
            var connectionManager = scope.ServiceProvider.GetRequiredService<IConnectionManager>();

            Console.WriteLine("已注册的连接:");
            foreach (var name in connectionManager.GetAllConnectionNames())
            {
                Console.WriteLine($"  - {name}");
            }

            Console.WriteLine();

            Console.WriteLine("发布消息到不同连接:");

            // 发布到默认连接
            await multiPublisher.PublishAsync(
                queueName: "default.queue",
                message: new MessageBody("Message to Default"u8.ToArray()),
                connectionName: "Default");

            Console.WriteLine("  ✅ 已发布到 Default 连接");

            // 发布到订单系统连接
            try
            {
                await multiPublisher.PublishAsync(
                    queueName: "order.queue",
                    message: new MessageBody("Message to OrderSystem"u8.ToArray()),
                    connectionName: "OrderSystem");

                Console.WriteLine("  ✅ 已发布到 OrderSystem 连接");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ❌ OrderSystem 连接失败: {ex.Message}");
            }

            // 发布到支付系统连接
            try
            {
                await multiPublisher.PublishAsync(
                    queueName: "payment.queue",
                    message: new MessageBody("Message to PaymentSystem"u8.ToArray()),
                    connectionName: "PaymentSystem");

                Console.WriteLine("  ✅ 已发布到 PaymentSystem 连接");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ❌ PaymentSystem 连接失败: {ex.Message}");
            }
        }

        Console.WriteLine("\n示例完成!");
    }
}
