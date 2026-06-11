using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Sample.WorkQueue;

public class EmailHandler : IMessageHandler
{
    private readonly ILogger<EmailHandler> _logger;

    public EmailHandler(ILogger<EmailHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("处理邮件: {Message}", message);
        await Task.Delay(500, cancellationToken);
        _logger.LogInformation("邮件处理完成");
        return HandleResult.SuccessResult;
    }
}

public class SmsHandler : IMessageHandler
{
    private readonly ILogger<SmsHandler> _logger;

    public SmsHandler(ILogger<SmsHandler> logger)
    {
        _logger = logger;
    }

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var message = System.Text.Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("处理短信: {Message}", message);
        await Task.Delay(300, cancellationToken);
        _logger.LogInformation("短信处理完成");
        return HandleResult.SuccessResult;
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== RabbitMQ 工作队列示例 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<EmailHandler>();
        builder.Services.AddScoped<SmsHandler>();

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "EmailConsumer1",
            QueueName = "email.notifications",
            ConsumerType = typeof(EmailHandler),
            PrefetchCount = 1
        });

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "EmailConsumer2",
            QueueName = "email.notifications",
            ConsumerType = typeof(EmailHandler),
            PrefetchCount = 1
        });

        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "SmsConsumer",
            QueueName = "sms.notifications",
            ConsumerType = typeof(SmsHandler),
            PrefetchCount = 1
        });

        builder.Services.AddHostedService<Hyz.RabbitMQ.Subscriber.RabbitMqSubscriberHost>();

        var host = builder.Build();

        Console.WriteLine("启动工作队列消费者...\n");

        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

            for (int i = 1; i <= 5; i++)
            {
                var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes($"邮件任务 #{i}"));
                await publisher.PublishAsync("email.notifications", message);
                Console.WriteLine($"已发送邮件任务 #{i}");
            }

            for (int i = 1; i <= 3; i++)
            {
                var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes($"短信任务 #{i}"));
                await publisher.PublishAsync("sms.notifications", message);
                Console.WriteLine($"已发送短信任务 #{i}");
            }
        }

        Console.WriteLine("\n按 Ctrl+C 退出...\n");

        await host.RunAsync();
    }
}
