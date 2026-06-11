namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 消费选项
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// 消费者标签
    /// </summary>
    public string? ConsumerTag { get; set; }

    /// <summary>
    /// 自动确认模式
    /// </summary>
    public bool AutoAck { get; set; } = false;

    /// <summary>
    /// 预取数量
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// 是否独占消费
    /// </summary>
    public bool Exclusive { get; set; } = false;

    /// <summary>
    /// 消费者优先级
    /// </summary>
    public byte Priority { get; set; } = 0;

    /// <summary>
    /// 是否自动删除队列
    /// </summary>
    public bool AutoDelete { get; set; } = false;

    /// <summary>
    /// 额外参数
    /// </summary>
    public IDictionary<string, object?>? Arguments { get; set; }
}
