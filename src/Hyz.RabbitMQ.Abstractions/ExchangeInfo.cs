namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 交换机类型
/// </summary>
public enum ExchangeType
{
    /// <summary>
    /// 直接交换机
    /// </summary>
    Direct,

    /// <summary>
    /// 扇出交换机
    /// </summary>
    Fanout,

    /// <summary>
    /// 主题交换机
    /// </summary>
    Topic,

    /// <summary>
    /// Headers 交换机
    /// </summary>
    Headers
}

/// <summary>
/// 交换机信息
/// </summary>
public class ExchangeInfo
{
    /// <summary>
    /// 交换机名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 交换机类型
    /// </summary>
    public ExchangeType Type { get; init; }

    /// <summary>
    /// 是否持久化
    /// </summary>
    public bool Durable { get; init; }

    /// <summary>
    /// 是否自动删除
    /// </summary>
    public bool AutoDelete { get; init; }

    /// <summary>
    /// 内部交换机
    /// </summary>
    public bool Internal { get; init; }

    /// <summary>
    /// 额外参数
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; init; }
}

/// <summary>
/// 队列信息
/// </summary>
public class QueueInfo
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 消息数量
    /// </summary>
    public uint MessageCount { get; init; }

    /// <summary>
    /// 消费者数量
    /// </summary>
    public uint ConsumerCount { get; init; }

    /// <summary>
    /// 活跃队列
    /// </summary>
    public string? LiveQueue { get; init; }
}
