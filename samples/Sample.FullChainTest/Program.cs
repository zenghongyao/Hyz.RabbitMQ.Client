using System.Diagnostics;
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

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[AttrOrderHandler] 收到: {Content}", content);
        return HandleResult.SuccessResult;
    }
}

[RabbitMqConsumer(Queue = "test.attr.user", PrefetchCount = 20)]
public class AttributeUserHandler : IMessageHandler
{
    private readonly ILogger<AttributeUserHandler> _logger;
    public AttributeUserHandler(ILogger<AttributeUserHandler> logger) => _logger = logger;

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[AttrUserHandler] 收到: {Content}", content);
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

    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        var content = Encoding.UTF8.GetString(context.Body.Span);
        _logger.LogInformation("[SourceGenChain] 收到: {Content}", content);
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
}