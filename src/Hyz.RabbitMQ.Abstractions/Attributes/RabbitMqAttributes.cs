namespace Hyz.RabbitMQ.Abstractions.Attributes;

/// <summary>
/// RabbitMQ 消费者特性 - 标记一个类为消息处理器
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqConsumerAttribute : Attribute
{
    /// <summary>
    /// 队列名称 (必需)
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// 交换机名称
    /// </summary>
    public string? Exchange { get; init; }

    /// <summary>
    /// 路由键
    /// </summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// 连接名称 (不填则使用默认连接)
    /// </summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    /// 消费者标签前缀
    /// </summary>
    public string? TagPrefix { get; init; }

    /// <summary>
    /// 自动确认模式
    /// </summary>
    public bool AutoAck { get; init; } = false;

    /// <summary>
    /// 预取数量
    /// </summary>
    public ushort PrefetchCount { get; init; } = 10;

    /// <summary>
    /// 是否持久化队列
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; init; } = 3;

    /// <summary>
    /// 死信交换机
    /// </summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>
    /// 死信路由键
    /// </summary>
    public string? DeadLetterRoutingKey { get; init; }
}

/// <summary>
/// 方法级订阅特性
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqSubscribeAttribute : Attribute
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// 交换机名称
    /// </summary>
    public string? Exchange { get; init; }

    /// <summary>
    /// 路由键
    /// </summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// 连接名称
    /// </summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    /// 自动确认模式
    /// </summary>
    public bool AutoAck { get; init; } = false;

    /// <summary>
    /// 预取数量
    /// </summary>
    public ushort PrefetchCount { get; init; } = 10;

    /// <summary>
    /// 是否持久化
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; init; } = 3;

    /// <summary>
    /// 死信交换机
    /// </summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>
    /// 死信路由键
    /// </summary>
    public string? DeadLetterRoutingKey { get; init; }

    /// <summary>
    /// 使用独立线程
    /// </summary>
    public bool UseDedicatedThread { get; init; } = true;

    /// <summary>
    /// 线程名称
    /// </summary>
    public string? ThreadName { get; init; }

    /// <summary>
    /// 启动优先级
    /// </summary>
    public int StartupPriority { get; init; } = 0;
}

/// <summary>
/// 批量消费方法特性
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqBatchSubscribeAttribute : Attribute
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// 批量大小
    /// </summary>
    public int BatchSize { get; init; } = 10;

    /// <summary>
    /// 批次超时(毫秒)
    /// </summary>
    public int BatchTimeoutMs { get; init; } = 1000;

    /// <summary>
    /// 交换机
    /// </summary>
    public string? Exchange { get; init; }

    /// <summary>
    /// 路由键
    /// </summary>
    public string? RoutingKey { get; init; }

    /// <summary>
    /// 连接名称
    /// </summary>
    public string? ConnectionName { get; init; }

    /// <summary>
    /// 预取数量
    /// </summary>
    public ushort PrefetchCount { get; init; } = 50;
}

/// <summary>
/// 交换机特性 - 标记在类上声明交换机
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqExchangeAttribute : Attribute
{
    /// <summary>
    /// 交换机名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 交换机类型
    /// </summary>
    public string Type { get; init; } = "Direct";

    /// <summary>
    /// 是否持久化
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// 自动删除
    /// </summary>
    public bool AutoDelete { get; init; } = false;

    /// <summary>
    /// 额外参数（JSON）
    /// </summary>
    public string? Arguments { get; init; }
}

/// <summary>
/// 队列特性 - 标记在类上声明队列
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqQueueAttribute : Attribute
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 是否持久化
    /// </summary>
    public bool Durable { get; init; } = true;

    /// <summary>
    /// 独占模式
    /// </summary>
    public bool Exclusive { get; init; } = false;

    /// <summary>
    /// 自动删除
    /// </summary>
    public bool AutoDelete { get; init; } = false;

    /// <summary>
    /// 消息 TTL
    /// </summary>
    public int? MessageTtl { get; init; }

    /// <summary>
    /// 最大队列长度
    /// </summary>
    public int? MaxLength { get; init; }

    /// <summary>
    /// 死信交换机
    /// </summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>
    /// 死信路由键
    /// </summary>
    public string? DeadLetterRoutingKey { get; init; }

    /// <summary>
    /// 额外参数（JSON）
    /// </summary>
    public string? Arguments { get; init; }
}

/// <summary>
/// 绑定特性 - 标记在类上声明绑定关系
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class RabbitMqBindingAttribute : Attribute
{
    /// <summary>
    /// 交换机名称
    /// </summary>
    public required string Exchange { get; init; }

    /// <summary>
    /// 路由键
    /// </summary>
    public required string RoutingKey { get; init; }

    /// <summary>
    /// 队列名称（可选，默认使用类上声明的第一个 [RabbitMqQueue] 的队列）
    /// </summary>
    public string? QueueName { get; init; }
}
