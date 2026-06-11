using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Abstractions.Attributes;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using IConsumerService = Hyz.RabbitMQ.Abstractions.IConsumerService;
using ConsumerService = Hyz.RabbitMQ.Core.ConsumerService;
using IMultiConnectionPublisherService = Hyz.RabbitMQ.Abstractions.IMultiConnectionPublisherService;

namespace Sample.Benchmarks;

public class Program
{
    public static void Main(string[] args)
    {
        RunManualBenchmark().GetAwaiter().GetResult();
    }

    static async Task RunManualBenchmark()
    {
        Console.WriteLine("======================================================================");
        Console.WriteLine("  Hyz.RabbitMQ 性能基准测试");
        Console.WriteLine("======================================================================\n");

        var benchmark = new RabbitMqBenchmarks();
        await benchmark.GlobalSetup();

        try
        {
            Console.WriteLine("开始性能测试...\n");

            // 单连接单发布
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await benchmark.SingleConnectionPublish();
            sw.Stop();
            Console.WriteLine($"  ✓ 单连接单发布: {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 单连接批量发布
            sw.Restart();
            await benchmark.SingleConnectionBatchPublish();
            sw.Stop();
            Console.WriteLine($"  ✓ 单连接批量发布 (100条): {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 单连接单消费
            sw.Restart();
            await benchmark.SingleConnectionConsume();
            sw.Stop();
            Console.WriteLine($"  ✓ 单连接单消费: {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 单连接批量消费
            sw.Restart();
            await benchmark.SingleConnectionBatchConsume();
            sw.Stop();
            Console.WriteLine($"  ✓ 单连接批量消费 (20条): {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 多连接发布
            sw.Restart();
            await benchmark.MultiConnectionPublish();
            sw.Stop();
            Console.WriteLine($"  ✓ 多连接单发布: {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 多连接批量发布
            sw.Restart();
            await benchmark.MultiConnectionBatchPublish();
            sw.Stop();
            Console.WriteLine($"  ✓ 多连接批量发布 (100条): {sw.Elapsed.TotalMilliseconds:F2} ms");

            // Job订阅消费
            sw.Restart();
            await benchmark.JobSubscriptionConsume();
            sw.Stop();
            Console.WriteLine($"  ✓ Job订阅消费: {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 特性订阅消费
            sw.Restart();
            await benchmark.AttributeSubscriptionConsume();
            sw.Stop();
            Console.WriteLine($"  ✓ 特性订阅消费: {sw.Elapsed.TotalMilliseconds:F2} ms");

            // 增量源码生成器订阅消费
            sw.Restart();
            await benchmark.GeneratedSubscriptionConsume();
            sw.Stop();
            Console.WriteLine($"  ✓ 源码生成器订阅消费: {sw.Elapsed.TotalMilliseconds:F2} ms");

            Console.WriteLine("\n======================================================================");
            Console.WriteLine("  所有基准测试完成!");
            Console.WriteLine("======================================================================");
        }
        finally
        {
            await benchmark.GlobalCleanup();
        }
    }
}

[MemoryDiagnoser]
[Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.FastestToSlowest)]
[RankColumn]
public class RabbitMqBenchmarks
{
    private const string HostName = "localhost";
    private const string UserName = "guest";
    private const string Password = "guest";
    private const int WarmupIterations = 3;
    private const int BenchmarkIterations = 10;
    private const int MessageCount = 100;

    private IServiceProvider? _singleConnectionSp;
    private IServiceProvider? _multiConnectionSp;
    private IServiceProvider? _subscriptionSp;
    private IHost? _subscriptionHost;
    private IHost? _generatedHost;
    private IConnectionManager? _singleConnectionManager;
    private IConnectionManager? _multiConnectionManager;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _singleConnectionSp = CreateSingleConnectionServices();
        _multiConnectionSp = CreateMultiConnectionServices();
        _subscriptionSp = CreateSubscriptionServices();

        _subscriptionHost = CreateSubscriptionHost();
        _generatedHost = CreateGeneratedHost();

        using var scope = _singleConnectionSp.CreateScope();
        _singleConnectionManager = scope.ServiceProvider.GetRequiredService<IConnectionManager>();

        using var scope2 = _multiConnectionSp.CreateScope();
        _multiConnectionManager = scope2.ServiceProvider.GetRequiredService<IConnectionManager>();

        await SetupQueues();
        Console.WriteLine("基准测试环境初始化完成!\n");
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        if (_singleConnectionSp is IDisposable disposable1) disposable1.Dispose();
        if (_multiConnectionSp is IDisposable disposable2) disposable2.Dispose();
        if (_subscriptionSp is IDisposable disposable3) disposable3.Dispose();
        _subscriptionHost?.Dispose();
        _generatedHost?.Dispose();
    }

    private IServiceProvider CreateSingleConnectionServices()
    {
        var services = new ServiceCollection();
        services.AddRabbitMq(options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
        });
        return services.BuildServiceProvider();
    }

    private IServiceProvider CreateMultiConnectionServices()
    {
        var services = new ServiceCollection();
        services.AddRabbitMq("Default", options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
        });
        services.AddRabbitMq("Secondary", options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.VirtualHost = "test";
            options.UserName = UserName;
            options.Password = Password;
        });
        return services.BuildServiceProvider();
    }

    private IServiceProvider CreateSubscriptionServices()
    {
        var services = new ServiceCollection();
        services.AddRabbitMq(options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
        });
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Error));
        return services.BuildServiceProvider();
    }

    private IHost CreateSubscriptionHost()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
        });
        builder.Services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Error));
        builder.Services.AddScoped<JobMessageHandler>();
        builder.Services.AddScoped<AttributeBenchmarkHandler>();
        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "JobBenchmarkConsumer",
            QueueName = "bench.job.queue",
            ConsumerType = typeof(JobMessageHandler),
            PrefetchCount = 50
        });
        builder.Services.AddSingleton(new SubscriberRegistration
        {
            Name = "AttrBenchmarkConsumer",
            QueueName = "bench.attr.queue",
            ConsumerType = typeof(AttributeBenchmarkHandler),
            PrefetchCount = 50
        });
        builder.Services.AddHostedService<RabbitMqSubscriberHost>();
        return builder.Build();
    }

    private IHost CreateGeneratedHost()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
        });
        builder.Services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Error));
        builder.Services.AddScoped<GeneratedBenchmarkHandler>();
        builder.Services.AddHostedService<GeneratedBenchmarkHost>();
        return builder.Build();
    }

    private async Task SetupQueues()
    {
        if (_singleConnectionManager == null) return;

        var provider = _singleConnectionManager.GetProvider("Default");

        // 声明所有需要的队列
        await provider.QueueManager.DeclareAsync("bench.single.publish", durable: true);
        await provider.QueueManager.DeclareAsync("bench.single.consume", durable: true);
        await provider.QueueManager.DeclareAsync("bench.job.queue", durable: true);
        await provider.QueueManager.DeclareAsync("bench.attr.queue", durable: true);
        await provider.QueueManager.DeclareAsync("bench.generated.queue", durable: true);
        await provider.QueueManager.DeclareAsync("bench.multi.default", durable: true);
        await provider.QueueManager.DeclareAsync("bench.multi.secondary", durable: true);

        // 清空队列
        try { await provider.QueueManager.PurgeAsync("bench.single.publish"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.single.consume"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.job.queue"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.attr.queue"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.generated.queue"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.multi.default"); } catch { }
        try { await provider.QueueManager.PurgeAsync("bench.multi.secondary"); } catch { }
    }

    #region Single Connection Tests

    [Benchmark]
    public async Task SingleConnectionPublish()
    {
        using var scope = _singleConnectionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

        var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes("Benchmark message"));
        await publisher.PublishAsync("bench.single.publish", message);
    }

    [Benchmark]
    public async Task SingleConnectionBatchPublish()
    {
        using var scope = _singleConnectionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

        var messages = Enumerable.Range(1, MessageCount)
            .Select(i => new MessageBody(System.Text.Encoding.UTF8.GetBytes($"Batch message #{i}")))
            .ToList();

        foreach (var msg in messages)
        {
            await publisher.PublishAsync("bench.single.publish", msg);
        }
    }

    [Benchmark]
    public async Task SingleConnectionConsume()
    {
        using var scope = _singleConnectionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
        var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();

        // 发布一条消息
        await publisher.PublishAsync("bench.single.consume",
            new MessageBody(System.Text.Encoding.UTF8.GetBytes("Consume test")));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in consumer.ConsumeAsync("bench.single.consume", cancellationToken: cts.Token))
        {
            await msg.AckAsync();
            break;
        }
    }

    [Benchmark]
    public async Task SingleConnectionBatchConsume()
    {
        using var scope = _singleConnectionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
        var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();

        // 预发布一批消息
        for (int i = 0; i < 20; i++)
        {
            await publisher.PublishAsync("bench.single.consume",
                new MessageBody(System.Text.Encoding.UTF8.GetBytes($"Batch consume #{i}")));
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var count = 0;
        await foreach (var batch in consumer.ConsumeBatchAsync("bench.single.consume",
            batchSize: 20, batchTimeoutMs: 100, cancellationToken: cts.Token))
        {
            foreach (var msg in batch)
            {
                await msg.AckAsync();
                count++;
            }
            break;
        }
    }

    #endregion

    #region Multi Connection Tests

    [Benchmark]
    public async Task MultiConnectionPublish()
    {
        using var scope = _multiConnectionSp!.CreateScope();
        var multiPublisher = scope.ServiceProvider.GetRequiredService<IMultiConnectionPublisherService>();

        var message = new MessageBody(System.Text.Encoding.UTF8.GetBytes("Multi connection message"));
        await multiPublisher.PublishAsync("bench.multi.default", message, "Default");
    }

    [Benchmark]
    public async Task MultiConnectionBatchPublish()
    {
        using var scope = _multiConnectionSp!.CreateScope();
        var multiPublisher = scope.ServiceProvider.GetRequiredService<IMultiConnectionPublisherService>();

        var messages = Enumerable.Range(1, MessageCount)
            .Select(i => new MessageBody(System.Text.Encoding.UTF8.GetBytes($"Multi batch #{i}")))
            .ToList();

        foreach (var msg in messages)
        {
            await multiPublisher.PublishAsync("bench.multi.default", msg, "Default");
        }
    }

    #endregion

    #region Subscription Tests

    [Benchmark]
    public async Task JobSubscriptionConsume()
    {
        if (_subscriptionHost == null) return;

        using var scope = _subscriptionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

        // 发布消息到 Job 订阅队列
        await publisher.PublishAsync("bench.job.queue",
            new MessageBody(System.Text.Encoding.UTF8.GetBytes("Job subscription test")));

        // 等待订阅者处理
        await Task.Delay(200);
    }

    [Benchmark]
    public async Task AttributeSubscriptionConsume()
    {
        if (_subscriptionHost == null) return;

        using var scope = _subscriptionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

        // 发布消息到特性订阅队列
        await publisher.PublishAsync("bench.attr.queue",
            new MessageBody(System.Text.Encoding.UTF8.GetBytes("Attribute subscription test")));

        // 等待订阅者处理
        await Task.Delay(200);
    }

    [Benchmark]
    public async Task GeneratedSubscriptionConsume()
    {
        if (_generatedHost == null) return;

        using var scope = _subscriptionSp!.CreateScope();
        var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

        // 发布消息到源码生成器订阅队列
        await publisher.PublishAsync("bench.generated.queue",
            new MessageBody(System.Text.Encoding.UTF8.GetBytes("Generated subscription test")));

        // 等待订阅者处理
        await Task.Delay(200);
    }

    #endregion
}

#region Handlers

public class JobMessageHandler : IMessageHandler
{
    private readonly ILogger<JobMessageHandler> _logger;

    public JobMessageHandler(ILogger<JobMessageHandler> logger)
    {
        _logger = logger;
    }

    public Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(HandleResult.SuccessResult);
    }
}

[RabbitMqConsumer(Queue = "bench.attr.queue", PrefetchCount = 50)]
public class AttributeBenchmarkHandler : IMessageHandler
{
    private readonly ILogger<AttributeBenchmarkHandler> _logger;

    public AttributeBenchmarkHandler(ILogger<AttributeBenchmarkHandler> logger)
    {
        _logger = logger;
    }

    public Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(HandleResult.SuccessResult);
    }
}

[RabbitMqConsumer(Queue = "bench.generated.queue", PrefetchCount = 50)]
public class GeneratedBenchmarkHandler : IMessageHandler
{
    private readonly ILogger<GeneratedBenchmarkHandler> _logger;

    public GeneratedBenchmarkHandler(ILogger<GeneratedBenchmarkHandler> logger)
    {
        _logger = logger;
    }

    public Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(HandleResult.SuccessResult);
    }
}

#endregion

#region Generated Host

public class GeneratedBenchmarkHost : RabbitMqSubscriberHost
{
    public GeneratedBenchmarkHost(
        IServiceProvider serviceProvider,
        IEnumerable<SubscriberRegistration> registrations)
        : base(serviceProvider, registrations)
    {
    }
}

#endregion
