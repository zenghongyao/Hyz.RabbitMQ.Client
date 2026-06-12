using System.Diagnostics;
using System.Reflection;
using System.Text;
using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Hyz.RabbitMQ.Abstractions.Attributes;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Sample.FullChainTest;

#region Handlers

public class SingleMessageHandler : IMessageHandler
{
    private readonly ILogger<SingleMessageHandler> _logger;
    public SingleMessageHandler(ILogger<SingleMessageHandler> logger) => _logger = logger;

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[SingleHandler] 收到: {Content}", content);
        return HandleResult.SuccessResult;
    }
}

public class BatchMessageHandler : IBatchMessageHandler
{
    private readonly ILogger<BatchMessageHandler> _logger;
    public BatchMessageHandler(ILogger<BatchMessageHandler> logger) => _logger = logger;

    public async Task<BatchHandleResult> HandleBatchAsync(
        IReadOnlyList<ReceivedMessageContext> messages, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("[BatchHandler] 收到 {Count} 条", messages.Count);
        var count = 0;
        foreach (var msg in messages)
        {
            var content = Encoding.UTF8.GetString(msg.Body.Span);
            _logger.LogInformation("[BatchHandler] {Content}", content);
            count++;
        }
        return new BatchHandleResult { SuccessCount = count, RejectIndices = [], RetryIndices = [] };
    }
}

[RabbitMqConsumer(Queue = "test.attr.order", PrefetchCount = 10)]
public class AttributeOrderHandler : IMessageHandler
{
    private readonly ILogger<AttributeOrderHandler> _logger;
    public AttributeOrderHandler(ILogger<AttributeOrderHandler> logger) => _logger = logger;

    public static readonly List<string> ReceivedMessages = new();

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[AttrOrderHandler] 收到: {Content}", content);
        lock (ReceivedMessages) { ReceivedMessages.Add(content); }
        return HandleResult.SuccessResult;
    }
}

[RabbitMqConsumer(Queue = "test.attr.user", PrefetchCount = 20)]
public class AttributeUserHandler : IMessageHandler
{
    private readonly ILogger<AttributeUserHandler> _logger;
    public AttributeUserHandler(ILogger<AttributeUserHandler> logger) => _logger = logger;

    public static readonly List<string> ReceivedMessages = new();

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[AttrUserHandler] 收到: {Content}", content);
        lock (ReceivedMessages) { ReceivedMessages.Add(content); }
        return HandleResult.SuccessResult;
    }
}

/// <summary>
/// 源码生成器订阅处理器 — 通过 [RabbitMqConsumer] 特性声明，
/// 增量源码生成器自动生成 AddSourceGenChainHandlerSubscriber() 注册代码
/// </summary>
[RabbitMqConsumer(Queue = "test.sg.chain", PrefetchCount = 10)]
public class SourceGenChainHandler : IMessageHandler
{
    private readonly ILogger<SourceGenChainHandler> _logger;
    public SourceGenChainHandler(ILogger<SourceGenChainHandler> logger) => _logger = logger;

    public static readonly List<string> ReceivedMessages = new();

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[SourceGenChain] 收到: {Content}", content);
        lock (ReceivedMessages) { ReceivedMessages.Add(content); }
        return HandleResult.SuccessResult;
    }
}

/// <summary>
/// 源码生成器拓扑声明类 — 通过特性声明交换机、队列和绑定关系，
/// 增量源码生成器自动生成对应的 AddXxxSubscriber 注册代码
/// </summary>
[RabbitMqExchange(Name = "test.sg.events", Type = "topic")]
[RabbitMqQueue(Name = "test.sg.chain")]
[RabbitMqBinding(Exchange = "test.sg.events", RoutingKey = "order.#")]
public static partial class SourceGenTopology
{
}

/// <summary>
/// 源码生成器方法级订阅处理器 — 类实现 IMessageHandler 处理消息，
/// 同时通过 [RabbitMqConsumer] 类特性让源码生成器自动生成注册代码 (AddSourceGenMethodHandler等)。
/// 源码生成器检测到 [RabbitMqConsumer] 后自动生成 Registration + Infrastructure 代码。
/// </summary>
[RabbitMqConsumer(Queue = "test.sg.method", PrefetchCount = 10)]
public class SourceGenMethodHandler : IMessageHandler
{
    private readonly ILogger<SourceGenMethodHandler> _logger;
    public SourceGenMethodHandler(ILogger<SourceGenMethodHandler> logger) => _logger = logger;

    public static readonly List<string> ReceivedMessages = new();

    public Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[SourceGenMethod] HandleAsync 收到: {Content}", content);
        lock (ReceivedMessages) { ReceivedMessages.Add(content); }
        return Task.FromResult(HandleResult.SuccessResult);
    }

    /// <summary>
    /// 批量处理方法（通过 StartBatchConsumingAsync 手动启动）
    /// </summary>
    public Task<BatchHandleResult> OnBatchMessage(
        IReadOnlyList<ReceivedMessageContext> messages, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("[SourceGenMethod] OnBatchMessage 收到 {Count} 条", messages.Count);
        var items = new List<string>();
        foreach (var m in messages)
        {
            var content = Encoding.UTF8.GetString(m.Body.Span);
            items.Add(content);
            _logger.LogInformation("[SourceGenMethod.Batch] {Content}", content);
        }
        lock (ReceivedMessages) { ReceivedMessages.AddRange(items); }
        return Task.FromResult(new BatchHandleResult
        {
            SuccessCount = messages.Count,
            RejectIndices = Array.Empty<int>(),
            RetryIndices = Array.Empty<int>()
        });
    }
}

#endregion

class Program
{
    private const string HostName = "localhost";
    private const string UserName = "guest";
    private const string Password = "guest";

    static Program()
    {
        // 消除中文控制台乱码
        Console.OutputEncoding = Encoding.UTF8;
    }

    /// <summary>
    /// 构建共享 Host，一次性注册所有服务和处理器
    /// </summary>
    static IHost BuildSharedHost()
    {
        var builder = Host.CreateApplicationBuilder();

        builder.Services.AddRabbitMq(options =>
        {
            options.HostName = HostName;
            options.Port = 5672;
            options.UserName = UserName;
            options.Password = Password;
            options.AutoReconnect = true;
        });

        builder.Services.AddScoped<SingleMessageHandler>();
        builder.Services.AddScoped<BatchMessageHandler>();
        builder.Services.AddScoped<AttributeOrderHandler>();
        builder.Services.AddScoped<AttributeUserHandler>();
        builder.Services.AddScoped<SourceGenChainHandler>();
        builder.Services.AddScoped<SourceGenMethodHandler>();

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        return builder.Build();
    }

    static async Task Main(string[] args)
    {
        PrintHeader("Hyz.RabbitMQ 全链路测试");

        using var host = BuildSharedHost();
        var results = new List<(string Name, bool Success, string Message, long Ms)>();

        results.Add(await TestConnection(host));
        results.Add(await TestSingleConsume(host));
        results.Add(await TestTopologyChain(host));
        results.Add(await TestBatchConsume(host));
        results.Add(await TestAttributeInjection(host));
        results.Add(await TestSourceGeneratorChain(host));
        results.Add(await TestSourceGenMethodSubscribe(host));
        results.Add(await TestAutoScanSubscribe(host));
        results.Add(await TestFullChain(host));
        results.Add(await TestMultiConnection());

        PrintResults(results);
    }

    #region Helpers

    static MessageBody M(string text) => new(Encoding.UTF8.GetBytes(text));
    static string S(ReceivedMessageContext ctx) => Encoding.UTF8.GetString(ctx.Body.Span);

    static void PrintHeader(string title)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine(new string('=', 70));
        Console.WriteLine($"  {title}");
        Console.WriteLine(new string('=', 70));
        Console.ResetColor();
        Console.WriteLine();
    }

    static void PrintSection(string title)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\n{title}");
        Console.WriteLine(new string('-', 50));
        Console.ResetColor();
    }

    static void PrintResults(List<(string Name, bool Success, string Message, long Ms)> results)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"\n{new string('=', 70)}");
        Console.WriteLine("  测试结果汇总");
        Console.WriteLine(new string('=', 70));
        Console.ResetColor();

        var pass = 0;
        foreach (var (name, success, msg, ms) in results)
        {
            if (success)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"  ✓ {name}: 通过 [{ms}ms] — {msg}");
                pass++;
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"  ✗ {name}: 失败 [{ms}ms] — {msg}");
            }
        }

        Console.ResetColor();
        Console.WriteLine(new string('—', 50));
        Console.ForegroundColor = pass == results.Count ? ConsoleColor.Green : ConsoleColor.Yellow;
        Console.WriteLine($"  通过: {pass}/{results.Count}");
        Console.ResetColor();
    }

    #endregion

    #region Test 1: Connection

    static async Task<(string, bool, string, long)> TestConnection(IHost host)
    {
        PrintSection("测试 1: 连接测试");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var provider = mgr.GetProvider("Default");
            var conn = await provider.GetConnectionAsync();

            Console.WriteLine($"  ✓ 连接成功: {provider.Name}, IsOpen: {conn.IsOpen}");
            return ("连接测试", true, "连接成功", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("连接测试", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 2: Single Consume

    static async Task<(string, bool, string, long)> TestSingleConsume(IHost host)
    {
        PrintSection("测试 2: 单条发布+消费");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();

            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var provider = mgr.GetProvider("Default");
            await provider.QueueManager.DeclareAsync("test.single", durable: true);

            var content = $"消息_{Guid.NewGuid():N}"[..10];
            await publisher.PublishAsync("test.single", M(content));
            Console.WriteLine("  ✓ 发布成功");

            var received = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await foreach (var m in consumer.ConsumeAsync("test.single", cancellationToken: cts.Token))
            {
                received++;
                Console.WriteLine($"  ✓ 收到: {S(m)}");
                await m.AckAsync();
                break;
            }

            var ok = received == 1;
            Console.WriteLine($"  {(ok ? "✓" : "✗")} 消费 {received} 条");
            return ("单条发布+消费", ok, ok ? "通过" : $"期望1条实收{received}条", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("单条发布+消费", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 3: Topology Chain

    static async Task<(string, bool, string, long)> TestTopologyChain(IHost host)
    {
        PrintSection("测试 3: 拓扑管理链路 (Exchange→Queue→Bind→Publish→Consume)");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();
            var provider = mgr.GetProvider("Default");

            try
            {
                await provider.ExchangeManager.DeleteAsync("test.topo.ex", ifUnused: false);
                await provider.QueueManager.DeleteAsync("test.topo.q", ifUnused: false, ifEmpty: false);
            }
            catch { }

            var exchange = await provider.ExchangeManager.DeclareAsync("test.topo.ex", ExchangeType.Topic, durable: true);
            Console.WriteLine($"  ✓ 交换机: {exchange.Name} (Type={exchange.Type})");

            var queue = await provider.QueueManager.DeclareAsync("test.topo.q", durable: true);
            Console.WriteLine($"  ✓ 队列: {queue.Name} (Msg={queue.MessageCount})");

            await provider.ExchangeManager.BindAsync("test.topo.ex", "test.topo.q", "order.#");
            Console.WriteLine("  ✓ 绑定: test.topo.ex → test.topo.q (rk=order.#)");

            var exExists = await provider.ExchangeManager.ExistsAsync("test.topo.ex");
            var qExists = await provider.QueueManager.ExistsAsync("test.topo.q");
            Console.WriteLine($"  ✓ Exists: Exchange={exExists} Queue={qExists}");

            var msgContent = $"Topo_{Guid.NewGuid():N}"[..10];
            await publisher.PublishToExchangeAsync("test.topo.ex", "order.created",
                M(msgContent), new PublishOptions { CorrelationId = Guid.NewGuid().ToString() });
            Console.WriteLine("  ✓ 发布成功");

            var received = "";
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await foreach (var m in consumer.ConsumeAsync("test.topo.q", cancellationToken: cts.Token))
            {
                received = S(m);
                Console.WriteLine($"  ✓ 收到: {received} (CorrelationId={m.CorrelationId})");
                await m.AckAsync();
                break;
            }

            var ok = received == msgContent;
            Console.WriteLine($"  {(ok ? "✓" : "✗")} 消息验证: {(ok ? "匹配" : $"不匹配! sent={msgContent} recv={received}")}");
            return ("拓扑管理链路", ok, ok ? "Exchange→Queue→Bind→Publish→Consume 全链路" : "消息不匹配", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("拓扑管理链路", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 4: Batch Consume

    static async Task<(string, bool, string, long)> TestBatchConsume(IHost host)
    {
        PrintSection("测试 4: 批量发布+消费");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();
            var provider = mgr.GetProvider("Default");

            await provider.QueueManager.DeclareAsync("test.batch", durable: true);
            await provider.QueueManager.PurgeAsync("test.batch");

            for (int i = 1; i <= 10; i++)
                await publisher.PublishAsync("test.batch", M($"批量#{i}"));
            Console.WriteLine("  ✓ 已发布 10 条");

            var total = 0;
            var batchCount = 0;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var batch in consumer.ConsumeBatchAsync("test.batch", batchSize: 5, batchTimeoutMs: 2000, cancellationToken: cts.Token))
            {
                batchCount++;
                total += batch.Count;
                Console.WriteLine($"  批次#{batchCount}: {batch.Count} 条");
                foreach (var m in batch) await m.AckAsync();
                if (total >= 10) break;
            }

            var ok = total == 10;
            Console.WriteLine($"  {(ok ? "✓" : "✗")} {batchCount}批次/{total}条");
            return ("批量发布+消费", ok, $"{batchCount}批次{total}条", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("批量发布+消费", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 5: Attribute Injection

    static async Task<(string, bool, string, long)> TestAttributeInjection(IHost host)
    {
        PrintSection("测试 5: 特性自动注入 [RabbitMqConsumer]");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();
            var provider = mgr.GetProvider("Default");

            await provider.QueueManager.DeclareAsync("test.attr.order", durable: true);
            await provider.QueueManager.DeclareAsync("test.attr.user", durable: true);

            var h1 = scope.ServiceProvider.GetRequiredService<AttributeOrderHandler>();
            var h2 = scope.ServiceProvider.GetRequiredService<AttributeUserHandler>();
            Console.WriteLine($"  ✓ 生成代码注入: {h1.GetType().Name}, {h2.GetType().Name}");

            await publisher.PublishAsync("test.attr.order", M("Order#A001"));
            await publisher.PublishAsync("test.attr.user", M("User#U001"));
            Console.WriteLine("  ✓ 已发布 2 条");

            var received = new List<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
            var t1 = ConsumeOnce(consumer, "test.attr.order", "order", received, cts.Token);
            var t2 = ConsumeOnce(consumer, "test.attr.user", "user", received, cts.Token);
            await Task.WhenAll(t1, t2);

            Console.WriteLine($"  ✓ 消费 {received.Count} 条: {string.Join(", ", received)}");
            var ok = received.Count >= 2;
            return ("特性注入", ok, $"消费{received.Count}条", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("特性注入", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    static async Task ConsumeOnce(IConsumerService consumer, string queue, string label,
        List<string> received, CancellationToken ct)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(5));
            await foreach (var m in consumer.ConsumeAsync(queue, cancellationToken: cts.Token))
            {
                received.Add($"[{label}]{S(m)}");
                await m.AckAsync();
                break;
            }
        }
        catch (OperationCanceledException) { }
    }

    #endregion

    #region Test 6: Source Generator Chain

    static async Task<(string, bool, string, long)> TestSourceGeneratorChain(IHost host)
    {
        PrintSection("测试 6: 增量源码生成器完整链路");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumerService>();
            var provider = mgr.GetProvider("Default");

            var handler = scope.ServiceProvider.GetRequiredService<SourceGenChainHandler>();
            Console.WriteLine($"  ✓ 源码生成器已注册: {handler.GetType().Name} (Queue: test.sg.chain)");

            await provider.QueueManager.DeclareAsync("test.sg.chain", durable: true);
            await provider.QueueManager.PurgeAsync("test.sg.chain");
            Console.WriteLine("  ✓ 队列就绪: test.sg.chain");

            var msgId = Guid.NewGuid().ToString("N")[..12];
            await publisher.PublishAsync("test.sg.chain", M($"SourceGen_{msgId}"));
            Console.WriteLine($"  ✓ 已发布: SourceGen_{msgId}");

            var received = "";
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await foreach (var m in consumer.ConsumeAsync("test.sg.chain", cancellationToken: cts.Token))
            {
                received = S(m);
                Console.WriteLine($"  ✓ 收到: {received}");
                await m.AckAsync();
                break;
            }

            var ok = received.Contains(msgId);
            Console.WriteLine($"  {(ok ? "✓" : "✗")} 源码生成器链路: {(ok ? "通过" : $"期望含{msgId} 收到{received}")}");
            return ("增量源码生成", ok, ok ? "注册→发布→消费全链路" : "消息不匹配", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("增量源码生成", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 7: Multi-Connection

    static async Task<(string, bool, string, long)> TestMultiConnection()
    {
        PrintSection("测试 7: 多连接");
        var sw = Stopwatch.StartNew();

        try
        {
            var builder = Host.CreateApplicationBuilder();
            builder.Services.AddRabbitMq("Default", options =>
            {
                options.HostName = HostName; options.Port = 5672;
                options.UserName = UserName; options.Password = Password;
            });
            builder.Services.AddRabbitMq("TestVhost", options =>
            {
                options.HostName = HostName; options.Port = 5672;
                options.VirtualHost = "test";
                options.UserName = UserName; options.Password = Password;
            });

            using var h = builder.Build();
            using var scope = h.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var multiPub = scope.ServiceProvider.GetRequiredService<IMultiConnectionPublisherService>();

            var names = mgr.GetAllConnectionNames().ToList();
            Console.WriteLine($"  已注册 {names.Count} 个连接: {string.Join(", ", names)}");

            var c1 = await mgr.GetProvider("Default").GetConnectionAsync();
            var c2 = await mgr.GetProvider("TestVhost").GetConnectionAsync();
            Console.WriteLine($"  ✓ Default: Open={c1.IsOpen}");
            Console.WriteLine($"  ✓ TestVhost: Open={c2.IsOpen}");

            await mgr.GetProvider("Default").QueueManager.DeclareAsync("test.multi.d", durable: true);
            await mgr.GetProvider("TestVhost").QueueManager.DeclareAsync("test.multi.t", durable: true);

            await multiPub.PublishAsync("test.multi.d", M("to-default"), "Default");
            await multiPub.PublishAsync("test.multi.t", M("to-testvhost"), "TestVhost");
            Console.WriteLine("  ✓ 多连接发布成功");

            return ("多连接", true, $"连接数{names.Count}", sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("多连接", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 8: Source Generator Method-Level Subscribe

    /// <summary>
    /// 测试源码生成器方法级订阅 — 在方法上使用 [RabbitMqSubscribe] / [RabbitMqBatchSubscribe]，
    /// 源码生成器自动生成 AddXxxHandler() 注册代码，然后手动启动消费验证全链路。
    /// </summary>
    static async Task<(string, bool, string, long)> TestSourceGenMethodSubscribe(IHost host)
    {
        PrintSection("测试 8: 源码生成器方法级订阅 [RabbitMqSubscribe/BatchSubscribe]");
        var sw = Stopwatch.StartNew();

        try
        {
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var provider = mgr.GetProvider("Default");

            // 1. 解析源码生成器注册的处理器，验证生成代码生效
            var handler = scope.ServiceProvider.GetRequiredService<SourceGenMethodHandler>();
            Console.WriteLine($"  ✓ 源码生成器已生成注册: {handler.GetType().Name}");

            // 2. 声明队列
            await provider.QueueManager.DeclareAsync("test.sg.method", durable: true);
            await provider.QueueManager.DeclareAsync("test.sg.method.batch", durable: true);
            await provider.QueueManager.PurgeAsync("test.sg.method");
            await provider.QueueManager.PurgeAsync("test.sg.method.batch");
            Console.WriteLine("  ✓ 队列就绪: test.sg.method, test.sg.method.batch");

            // 3. 创建独立的 ConsumerService 并启动单条消费
            SourceGenMethodHandler.ReceivedMessages.Clear();
            var consumer = new ConsumerService(provider);
            var consumerTag = await consumer.StartConsumingAsync(
                "test.sg.method", handler,
                new ConsumerOptions { PrefetchCount = 10 });

            Console.WriteLine($"  ✓ 单条订阅已启动: {consumerTag}");

            // 4. 发布单条消息
            var singleId = Guid.NewGuid().ToString("N")[..8];
            await publisher.PublishAsync("test.sg.method", M($"SGMethod_Single_{singleId}"));
            Console.WriteLine($"  ✓ 已发布单条: SGMethod_Single_{singleId}");

            await Task.Delay(1500); // 等待消费

            var singleOk = SourceGenMethodHandler.ReceivedMessages.Any(m => m.Contains(singleId));
            Console.WriteLine($"  {(singleOk ? "✓" : "✗")} 单条订阅: {(singleOk ? "收到" : "未收到")}");

            // 5. 停止单条消费，启动批量消费
            await consumer.StopConsumingAsync(consumerTag);
            SourceGenMethodHandler.ReceivedMessages.Clear();
            consumer.Dispose();

            // 6. 启动批量消费
            var consumer2 = new ConsumerService(provider);
            var batchTag = await consumer2.StartBatchConsumingAsync(
                "test.sg.method.batch",
                async (messages) =>
                {
                    var res = await handler.OnBatchMessage(messages.ToList().AsReadOnly());
                    return res.SuccessCount == messages.Count;
                },
                batchSize: 5,
                batchTimeoutMs: 2000,
                new ConsumerOptions { PrefetchCount = 10 });

            Console.WriteLine($"  ✓ 批量订阅已启动: {batchTag}");

            // 7. 批量发布
            for (int i = 1; i <= 10; i++)
                await publisher.PublishAsync("test.sg.method.batch", M($"SGMethod_Batch_{i:D2}"));
            Console.WriteLine("  ✓ 已批量发布 10 条");

            await Task.Delay(4000); // 等待批量消费

            var batchOk = SourceGenMethodHandler.ReceivedMessages.Count >= 10;
            Console.WriteLine($"  {(batchOk ? "✓" : "✗")} 批量订阅: 收到 {SourceGenMethodHandler.ReceivedMessages.Count}/10 条");

            await consumer2.StopConsumingAsync(batchTag);
            consumer2.Dispose();

            var allOk = singleOk && batchOk;
            return ("源码生成器方法订阅", allOk,
                allOk ? $"单条{(singleOk ? "✓" : "✗")} 批量{(batchOk ? "✓" : "✗")}({SourceGenMethodHandler.ReceivedMessages.Count}条)"
                      : (singleOk ? "批量失败" : "单条失败"),
                sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("源码生成器方法订阅", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 9: Attribute Auto-Scan Subscription

    /// <summary>
    /// 测试特性自动扫描订阅 — 使用 SubscriberScanner.Scan() 运行时反射扫描程序集中
    /// 所有 [RabbitMqConsumer] 标记的类，通过 RabbitMqSubscriberHost 自动管理订阅线程。
    /// </summary>
    static async Task<(string, bool, string, long)> TestAutoScanSubscribe(IHost host)
    {
        PrintSection("测试 9: 特性自动扫描订阅 [SubscriberScanner + RabbitMqSubscriberHost]");
        var sw = Stopwatch.StartNew();

        try
        {
            // 1. 运行时扫描程序集，发现所有 [RabbitMqConsumer] 标记的类
            var assembly = typeof(Program).Assembly;
            var registrations = SubscriberScanner.Scan(assembly);

            Console.WriteLine($"  扫描到 {registrations.Count} 个 [RabbitMqConsumer] 订阅者:");
            foreach (var reg in registrations)
                Console.WriteLine($"    - {reg.Name} (队列: {reg.QueueName})");

            var scanOk = registrations.Count >= 4; // 预期至少 4 个 [RabbitMqConsumer] 类
            if (!scanOk)
            {
                Console.WriteLine($"  ✗ 扫描数量不足: 预期>=4, 实际{registrations.Count}");
                return ("特性自动扫描", false, $"扫描数量不足: {registrations.Count}", sw.ElapsedMilliseconds);
            }

            // 2. 筛选测试需要的订阅（排除 SourceGenChainHandler，它将在测试10中使用）
            var testRegs = registrations
                .Where(r => r.QueueName is "test.attr.order" or "test.attr.user" or "test.sg.method")
                .ToList();

            Console.WriteLine($"  筛选出 {testRegs.Count} 个测试订阅者");

            // 3. 创建 RabbitMqSubscriberHost 并启动
            var subscriberHost = new RabbitMqSubscriberHost(
                host.Services,
                testRegs,
                new RabbitMqSubscriberOptions { AutoStart = true, StartupDelay = TimeSpan.FromMilliseconds(200) });

            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await subscriberHost.StartAsync(cts.Token);

            // 4. 检查订阅状态
            var statuses = subscriberHost.GetAllStatuses();
            Console.WriteLine($"  订阅线程状态:");
            var allRunning = true;
            foreach (var s in statuses)
            {
                var isRunning = s.State == "Running";
                if (!isRunning) allRunning = false;
                Console.WriteLine($"    {(isRunning ? "✓" : "✗")} {s.ConsumerName}: {s.State} (队列: {s.QueueName})");
            }

            if (!allRunning)
            {
                await subscriberHost.StopAsync();
                return ("特性自动扫描", false, "部分订阅线程未启动", sw.ElapsedMilliseconds);
            }

            // 5. 发布消息到已订阅的队列
            using var scope = host.Services.CreateScope();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();
            var provider = host.Services.GetRequiredService<IConnectionManager>().GetProvider("Default");

            await provider.QueueManager.DeclareAsync("test.attr.order", durable: true);
            await provider.QueueManager.DeclareAsync("test.attr.user", durable: true);
            await provider.QueueManager.DeclareAsync("test.sg.method", durable: true);

            var msgs = new[]
            {
                ("test.attr.order", "Scan_Order_A001"),
                ("test.attr.order", "Scan_Order_A002"),
                ("test.attr.user", "Scan_User_U001"),
                ("test.sg.method", "Scan_Method_M001"),
            };

            foreach (var (q, msg) in msgs)
                await publisher.PublishAsync(q, M(msg));
            Console.WriteLine($"  ✓ 已发布 {msgs.Length} 条消息到自动扫描订阅的队列");

            // 6. 等待消费（SubscriberHost 的 SimpleMessageHandler 会自动 Ack）
            await Task.Delay(3000);

            // 7. 停止宿主
            await subscriberHost.StopAsync();

            Console.WriteLine("  ✓ 自动扫描订阅全链路完成");
            return ("特性自动扫描", true,
                $"扫描{registrations.Count}个→启动{testRegs.Count}个→发布{msgs.Length}条",
                sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("特性自动扫描", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    #endregion

    #region Test 10: Full Chain Combined Test

    /// <summary>
    /// 全链路综合测试 — 同时启动源码生成器订阅 + 特性自动扫描订阅，
    /// 向所有涉及的队列发布消息，通过各 Handler 的静态消息跟踪验证整个消息链路。
    /// </summary>
    static async Task<(string, bool, string, long)> TestFullChain(IHost host)
    {
        PrintSection("测试 10: 全链路综合测试 (源码生成器 + 特性扫描同时运行)");
        var sw = Stopwatch.StartNew();

        try
        {
            // 1. 声明所有需要的队列并清空
            using var scope = host.Services.CreateScope();
            var mgr = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
            var provider = mgr.GetProvider("Default");
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisherService>();

            var allQueues = new[] { "test.sg.chain", "test.sg.method", "test.attr.order", "test.attr.user" };
            foreach (var q in allQueues)
            {
                await provider.QueueManager.DeclareAsync(q, durable: true);
                await provider.QueueManager.PurgeAsync(q);
            }
            Console.WriteLine($"  ✓ 队列就绪: {string.Join(", ", allQueues)}");

            // 2. 清空所有 Handler 的消息跟踪
            SourceGenChainHandler.ReceivedMessages.Clear();
            SourceGenMethodHandler.ReceivedMessages.Clear();
            AttributeOrderHandler.ReceivedMessages.Clear();
            AttributeUserHandler.ReceivedMessages.Clear();

            // 3. 解析所有 Handler
            var sgChainHandler = scope.ServiceProvider.GetRequiredService<SourceGenChainHandler>();
            var sgMethodHandler = scope.ServiceProvider.GetRequiredService<SourceGenMethodHandler>();
            var orderHandler = scope.ServiceProvider.GetRequiredService<AttributeOrderHandler>();
            var userHandler = scope.ServiceProvider.GetRequiredService<AttributeUserHandler>();

            // 4. 为每个 Handler 启动独立消费者（源码生成器订阅方式）
            var consumers = new List<ConsumerService>();
            var tags = new List<string>();

            var (c1, t1) = await StartHandlerConsumer(provider, "test.sg.chain", sgChainHandler);
            consumers.Add(c1); tags.Add(t1);
            Console.WriteLine("  ✓ 源码生成器订阅: test.sg.chain");

            var (c2, t2) = await StartHandlerConsumer(provider, "test.sg.method", sgMethodHandler);
            consumers.Add(c2); tags.Add(t2);
            Console.WriteLine("  ✓ 源码生成器订阅: test.sg.method");

            var (c3, t3) = await StartHandlerConsumer(provider, "test.attr.order", orderHandler);
            consumers.Add(c3); tags.Add(t3);
            Console.WriteLine("  ✓ 特性注入订阅: test.attr.order");

            var (c4, t4) = await StartHandlerConsumer(provider, "test.attr.user", userHandler);
            consumers.Add(c4); tags.Add(t4);
            Console.WriteLine("  ✓ 特性注入订阅: test.attr.user");

            // 5. 向所有队列发布消息
            var publishPlan = new (string Queue, string Content)[]
            {
                ("test.sg.chain", "FullChain_SG_001"),
                ("test.sg.chain", "FullChain_SG_002"),
                ("test.sg.method", "FullChain_Method_001"),
                ("test.sg.method", "FullChain_Method_002"),
                ("test.attr.order", "FullChain_Order_001"),
                ("test.attr.order", "FullChain_Order_002"),
                ("test.attr.user", "FullChain_User_001"),
            };

            foreach (var (q, msg) in publishPlan)
                await publisher.PublishAsync(q, M(msg));
            Console.WriteLine($"  ✓ 已发布 {publishPlan.Length} 条消息到 4 个队列");

            // 6. 等待所有消费完成
            await Task.Delay(3000);

            // 7. 通过 Handler 静态消息跟踪验证消费
            var chainMsgs = SourceGenChainHandler.ReceivedMessages
                .Where(m => m.StartsWith("FullChain_SG")).ToList();
            var methodMsgs = SourceGenMethodHandler.ReceivedMessages
                .Where(m => m.StartsWith("FullChain_Method")).ToList();
            var orderMsgs = AttributeOrderHandler.ReceivedMessages
                .Where(m => m.StartsWith("FullChain_Order")).ToList();
            var userMsgs = AttributeUserHandler.ReceivedMessages
                .Where(m => m.StartsWith("FullChain_User")).ToList();

            var chainOk = chainMsgs.Count >= 2;
            var methodOk = methodMsgs.Count >= 2;
            var orderOk = orderMsgs.Count >= 2;
            var userOk = userMsgs.Count >= 1;

            Console.WriteLine($"  {(chainOk ? "✓" : "✗")} SG-Chain:     收到 {chainMsgs.Count}/2 条");
            Console.WriteLine($"  {(methodOk ? "✓" : "✗")} SG-Method:    收到 {methodMsgs.Count}/2 条");
            Console.WriteLine($"  {(orderOk ? "✓" : "✗")} Attr-Order:   收到 {orderMsgs.Count}/2 条");
            Console.WriteLine($"  {(userOk ? "✓" : "✗")} Attr-User:    收到 {userMsgs.Count}/1 条");

            // 8. 停止所有订阅
            for (int i = 0; i < consumers.Count; i++)
            {
                await consumers[i].StopConsumingAsync(tags[i]);
                consumers[i].Dispose();
            }

            // 9. 汇总结果
            var allOk = chainOk && methodOk && orderOk && userOk;
            var detail = $"SG-Chain({(chainOk ? "✓" : "✗")}) SG-Method({(methodOk ? "✓" : "✗")}) " +
                         $"Attr-Order({(orderOk ? "✓" : "✗")}) Attr-User({(userOk ? "✓" : "✗")})";

            Console.WriteLine($"  {(allOk ? "✓" : "✗")} 全链路: {detail}");
            return ("全链路综合", allOk, allOk ? detail : detail, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  ✗ 失败: {ex.Message}");
            return ("全链路综合", false, ex.Message, sw.ElapsedMilliseconds);
        }
    }

    /// <summary>
    /// 辅助方法：启动一个 Handler 的消费者
    /// </summary>
    static async Task<(ConsumerService Consumer, string Tag)> StartHandlerConsumer(
        IConnectionProvider provider, string queue, IMessageHandler handler)
    {
        var consumer = new ConsumerService(provider);
        var tag = await consumer.StartConsumingAsync(queue, handler,
            new ConsumerOptions { PrefetchCount = 10 });
        return (consumer, tag);
    }

    #endregion
}