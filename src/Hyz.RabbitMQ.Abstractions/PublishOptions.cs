namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 发布选项
/// </summary>
public class PublishOptions
{
    /// <summary>
    /// 消息持久化
    /// </summary>
    public bool Persistent { get; set; } = true;

    /// <summary>
    /// 内容类型
    /// </summary>
    public string? ContentType { get; set; }

    /// <summary>
    /// 内容编码
    /// </summary>
    public string? ContentEncoding { get; set; }

    /// <summary>
    /// 自定义头
    /// </summary>
    public IDictionary<string, object?>? Headers { get; set; }

    /// <summary>
    /// 关联 ID (用于 RPC)
    /// </summary>
    public string? CorrelationId { get; set; }

    /// <summary>
    /// 回复队列
    /// </summary>
    public string? ReplyTo { get; set; }

    /// <summary>
    /// 消息过期时间
    /// </summary>
    public TimeSpan? Expiration { get; set; }

    /// <summary>
    /// 消息优先级 (0-9)
    /// </summary>
    public byte Priority { get; set; }

    /// <summary>
    /// 消息 ID
    /// </summary>
    public string? MessageId { get; set; }

    /// <summary>
    /// 时间戳
    /// </summary>
    public DateTimeOffset? Timestamp { get; set; }

    /// <summary>
    /// 生产者 ID
    /// </summary>
    public string? ProducerId { get; set; }

    /// <summary>
    /// mandatory 标志 (消息不可路由时是否返回)
    /// </summary>
    public bool Mandatory { get; set; } = false;
}
