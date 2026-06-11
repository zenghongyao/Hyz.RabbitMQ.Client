using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hyz.RabbitMQ.Subscriber;

/// <summary>
/// 订阅工作者，负责在后台独立运行一个消息消费循环。
/// 每个 SubscriptionWorker 实例绑定一个 SubscriberRegistration 配置，
/// 在 StartAsync 时创建独立的 DI Scope 并启动消费线程，
/// StopAsync 时优雅停止并清理资源。
/// </summary>
public class SubscriptionWorker
{
    private readonly SubscriberRegistration _registration;
    private readonly IServiceProvider _serviceProvider;
    private readonly RabbitMqSubscriberOptions _options;
    private readonly ILogger _logger;

    /// <summary>
    /// 当前工作线程的状态字符串，用于 GetStatus() 查询。
    /// 状态流转：Created → Running → Stopping → Stopped（正常停止）或 Error（异常）。
    /// </summary>
    private string _state = "Created";

    /// <summary>
    /// 此工作者已处理的消息总数（预留，供 SubscriptionStatus 快照使用）。
    /// </summary>
#pragma warning disable CS0649 // TODO: 在真实消息处理链路中每处理一条消息递增
    private int _processedCount;
#pragma warning restore CS0649

    /// <summary>
    /// 内部取消令牌源，用于在 StopAsync 时终止消费循环。
    /// </summary>
    private CancellationTokenSource? _cts;

    /// <summary>
    /// 此工作者的名称，来源于 SubscriberRegistration.Name。
    /// </summary>
    public string Name => _registration.Name;

    /// <summary>
    /// 创建订阅工作者实例。
    /// </summary>
    /// <param name="registration">订阅配置，包含队列名、连接名、预取数等。</param>
    /// <param name="serviceProvider">DI 服务提供器，用于解析消息处理器等服务。</param>
    /// <param name="options">订阅选项。</param>
    public SubscriptionWorker(
        SubscriberRegistration registration,
        IServiceProvider serviceProvider,
        RabbitMqSubscriberOptions? options = null)
    {
        _registration = registration;
        _serviceProvider = serviceProvider;
        _options = options ?? new RabbitMqSubscriberOptions();
        _logger = serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<SubscriptionWorker>()
            ?? NullLogger<SubscriptionWorker>.Instance;
    }

    /// <summary>
    /// 启动订阅线程。
    /// 创建内部 CancellationTokenSource 并启动异步消费循环，直到调用 StopAsync 或外部取消。
    /// </summary>
    /// <param name="cancellationToken">外部取消令牌（通常为 ApplicationStopping）。</param>
    /// <returns>表示启动完成的 Task。</returns>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _state = "Running";

        _logger.LogInformation("订阅线程已启动: {Name}", Name);

        try
        {
            await ConsumeAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            _state = "Stopped";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "订阅线程异常: {Name}", Name);
            _state = "Error";
        }
    }

    /// <summary>
    /// 停止订阅线程。
    /// 通过取消内部 CancellationTokenSource 实现优雅停止。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示停止完成的 Task。</returns>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        _state = "Stopping";
        _cts?.Cancel();
        _state = "Stopped";
        _logger.LogInformation("订阅线程已停止: {Name}", Name);
        return Task.CompletedTask;
    }

    /// <summary>
    /// 获取当前订阅状态快照，包含工作线程状态、已处理消息数等。
    /// </summary>
    /// <returns>SubscriptionStatus 实例。</returns>
    public SubscriptionStatus GetStatus()
    {
        return new SubscriptionStatus
        {
            ConsumerName = Name,
            QueueName = _registration.QueueName,
            State = _state,
            ConnectionName = _registration.ConnectionName,
            ProcessedCount = _processedCount
        };
    }

    /// <summary>
    /// 内部消费循环。在独立 DI Scope 中解析 IConnectionManager，
    /// 创建 ConsumerService，调用 StartConsuming 启动事件驱动消费，
    /// 然后通过 Task.Delay(Timeout.Infinite) 保持运行直到被取消。
    /// </summary>
    /// <param name="cancellationToken">内部取消令牌，由 StartAsync / StopAsync 控制。</param>
    private async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        // 创建独立 Scope，使消息处理器的依赖注入独立于主请求
        using var scope = _serviceProvider.CreateScope();
        var connectionManager = scope.ServiceProvider.GetRequiredService<IConnectionManager>();
        var provider = connectionManager.GetProvider(_registration.ConnectionName);

        // 预初始化连接：确保连接已建立后再启动消费
        await provider.GetConnectionAsync(cancellationToken);

        var consumer = new Core.ConsumerService(provider);

        var consumerTag = await consumer.StartConsumingAsync(
            _registration.QueueName,
            new SimpleMessageHandler(_registration, _logger),
            new ConsumerOptions { PrefetchCount = _registration.PrefetchCount },
            cancellationToken);

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        finally
        {
            await consumer.StopConsumingAsync(consumerTag);
        }
    }
}

/// <summary>
/// 简单消息处理器，当前实现为占位符（不做实际处理，直接返回成功）。
/// 在实际应用中，可通过 DI 解析真实的消息处理器服务。
/// </summary>
internal class SimpleMessageHandler : IMessageHandler
{
    private readonly SubscriberRegistration _registration;
    private readonly ILogger _logger;

    /// <summary>
    /// 创建简单消息处理器。
    /// </summary>
    /// <param name="registration">订阅配置（当前未使用，保留用于扩展）。</param>
    /// <param name="logger">日志记录器。</param>
    public SimpleMessageHandler(SubscriberRegistration registration, ILogger logger)
    {
        _registration = registration;
        _logger = logger;
    }

    /// <summary>
    /// 处理接收到的消息。当前直接返回成功结果。
    /// 实际应用中应在此方法中解析具体业务 Handler 并执行处理逻辑。
    /// </summary>
    /// <param name="context">接收消息上下文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>处理结果（当前固定返回成功）。</returns>
    public async Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            // 此处应实现实际的消息处理逻辑，例如：
            // var handler = serviceProvider.GetRequiredService<IMyMessageHandler>();
            // return await handler.HandleAsync(context, cancellationToken);
            return HandleResult.SuccessResult;
        }
        catch (Exception)
        {
            return HandleResult.Reject();
        }
    }
}
