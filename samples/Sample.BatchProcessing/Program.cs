using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;

namespace Sample.BatchProcessing;

public class BatchOrderHandler : IBatchMessageHandler
{
    private readonly ILogger<BatchOrderHandler> _logger;

    public BatchOrderHandler(ILogger<BatchOrderHandler> logger)
    {
        _logger = logger;
    }

    public async Task<BatchHandleResult> HandleBatchAsync(
        IReadOnlyList<ReceivedMessageContext> messages,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("收到批量消息: {Count} 条", messages.Count);

        var rejectIndices = new List<int>();
        var retryIndices = new List<int>();
        var successCount = 0;

        foreach (var (message, index) in messages.Select((m, i) => (m, i)))
        {
            try
            {
                var content = System.Text.Encoding.UTF8.GetString(message.Body.Span);
                _logger.LogInformation("处理消息 #{Index}: {Content}", index + 1, content);
                await Task.Delay(50, cancellationToken);
                successCount++;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "消息 #{Index} 处理失败", index + 1);
                retryIndices.Add(index);
            }
        }

        _logger.LogInformation("批量处理完成: 成功 {Success} 条, 重试 {Retry} 条",
            successCount, retryIndices.Count);

        return new BatchHandleResult
        {
            SuccessCount = successCount,
            RejectIndices = rejectIndices,
            RetryIndices = retryIndices
        };
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("=== RabbitMQ 批量处理示例 ===\n");

        var builder = Host.CreateApplicationBuilder(args);

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
            options.Port = 5672;
            options.UserName = "guest";
            options.Password = "guest";
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<BatchOrderHandler>();
        builder.Services.AddScoped<IConsumerService, ConsumerService>();

        var host = builder.Build();

        Console.WriteLine("启动批量处理示例...\n");

        using (var scope = host.Services.CreateScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

            for (int i = 1; i <= 20; i++)
            {
                var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes($"订单 #{i} - ¥{100 * i}"));
                await publisher.PublishAsync("batch.orders", message);
            }
            Console.WriteLine("已发布 20 条订单消息");
        }

        using (var scope = host.Services.CreateScope())
        {
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();

            Console.WriteLine("\n开始批量消费 (批量大小: 5, 超时: 3秒)...\n");

            var batchCount = 0;
            await foreach (var batch in consumer.ConsumeBatchAsync(
                "batch.orders",
                batchSize: 5,
                batchTimeoutMs: 3000))
            {
                batchCount++;
                Console.WriteLine($"收到批次 #{batchCount}: {batch.Count} 条消息");

                foreach (var msg in batch)
                {
                    var content = System.Text.Encoding.UTF8.GetString(msg.Body.Span);
                    Console.WriteLine($"处理: {content}");
                    await msg.AckAsync();
                }

                if (batchCount >= 4) break;
            }
        }

        Console.WriteLine("\n批量处理示例完成！");
    }
}
