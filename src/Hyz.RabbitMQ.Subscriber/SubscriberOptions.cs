using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Subscriber;

/// <summary>
/// 订阅扫描配置
/// </summary>
public class RabbitMqSubscriberOptions
{
    /// <summary>
    /// 自动启动 (默认 true)
    /// </summary>
    public bool AutoStart { get; set; } = true;

    /// <summary>
    /// 启动延迟
    /// </summary>
    public TimeSpan StartupDelay { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// 健康检查间隔
    /// </summary>
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromMinutes(1);
}

/// <summary>
/// 重试策略
/// </summary>
public enum RetryStrategy
{
    /// <summary>
    /// 固定间隔
    /// </summary>
    Fixed,
    /// <summary>
    /// 线性递增
    /// </summary>
    Linear,
    /// <summary>
    /// 指数退避
    /// </summary>
    Exponential
}

/// <summary>
/// 订阅注册信息
/// </summary>
public class SubscriberRegistration
{
    /// <summary>
    /// 订阅名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 连接名称
    /// </summary>
    public string ConnectionName { get; set; } = "Default";

    /// <summary>
    /// 队列名称
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// 交换机名称
    /// </summary>
    public string ExchangeName { get; set; } = string.Empty;

    /// <summary>
    /// 路由键
    /// </summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// 消费者类型 (用于 DI 解析)
    /// </summary>
    public required Type ConsumerType { get; init; }

    /// <summary>
    /// 启动优先级
    /// </summary>
    public int StartupPriority { get; set; }

    /// <summary>
    /// 启用重试
    /// </summary>
    public bool EnableRetry { get; set; } = true;

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 重试策略
    /// </summary>
    public RetryStrategy RetryStrategy { get; set; } = RetryStrategy.Exponential;

    /// <summary>
    /// 基础重试延迟 (毫秒)
    /// </summary>
    public int BaseRetryDelayMs { get; set; } = 1000;

    /// <summary>
    /// Prefetch 数量
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// 自动确认模式
    /// </summary>
    public bool AutoAck { get; set; }

    /// <summary>
    /// 是否持久化
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// 死信交换机
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// 死信路由键
    /// </summary>
    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>
    /// 消费者标签前缀
    /// </summary>
    public string? TagPrefix { get; set; }
}

/// <summary>
/// 订阅状态，记录订阅线程的运行时信息快照。
/// </summary>
public class SubscriptionStatus
{
    /// <summary>消费者名称。</summary>
    public string ConsumerName { get; set; } = string.Empty;
    /// <summary>正在消费的队列名称。</summary>
    public string QueueName { get; set; } = string.Empty;
    /// <summary>当前状态（Created / Running / Stopping / Stopped / Error）。</summary>
    public string State { get; set; } = "Unknown";
    /// <summary>使用的连接名称。</summary>
    public string ConnectionName { get; set; } = string.Empty;
    /// <summary>已处理的消息总数。</summary>
    public int ProcessedCount { get; set; }
    /// <summary>最后处理一条消息的时间。</summary>
    public DateTime? LastProcessedTime { get; set; }
    /// <summary>最近一次处理异常的错误信息。</summary>
    public string? LastError { get; set; }
}
