namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 接收消息上下文，封装消费者从 RabbitMQ 队列接收到的单条消息的全部信息。
/// 用于在消息处理回调中访问消息内容、手动确认/拒绝等操作。
/// </summary>
public class ReceivedMessageContext
{
    /// <summary>
    /// 消息的唯一标识符，由发布方在 PublishOptions.MessageId 中指定，
    /// 未指定时 PublisherService 会自动生成一个 GUID。
    /// </summary>
    public string? MessageId { get; init; }

    /// <summary>
    /// 消息体的字节数组。消费者通过 Body 属性读取消息的实际载荷，
    /// 可使用 MessageBody.Bytes 或 Encoding.UTF8.GetString() 转换。
    /// </summary>
    public required ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>
    /// 消息发布时指定的路由键（Routing Key）。
    /// 在 direct / topic 类型的交换机中，路由键决定消息被路由到哪个队列。
    /// </summary>
    public required string RoutingKey { get; init; }

    /// <summary>
    /// 消息被投递到的交换机名称。用于标识消息来源的交换机。
    /// </summary>
    public required string ExchangeName { get; init; }

    /// <summary>
    /// 接收此消息的队列名称。通过该字段可确认消息所在的队列。
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// 消息头（Headers）字典，用于在 AMQP 消息属性中传递自定义元数据。
    /// 可存储追踪 ID、租户 ID、版本号等业务扩展字段。
    /// </summary>
    public IDictionary<string, object?>? Headers { get; init; }

    /// <summary>
    /// 消息的内容类型（Content-Type），如 "application/json"、"application/x-msgpack"。
    /// 用于消费者在反序列化时选择正确的序列化器。
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// 指示此消息是否曾被消费者拒绝（Nack）后重新投递。
    /// true 表示该消息之前处理失败过，false 表示首次投递。
    /// </summary>
    public bool Redelivered { get; init; }

    /// <summary>
    /// RabbitMQ 为每条消息分配的递增序号，用于 BasicAck / BasicNack 确认操作。
    /// 每个 Channel 内的 DeliveryTag 唯一且单调递增。
    /// </summary>
    public required ulong DeliveryTag { get; init; }

    /// <summary>
    /// 关联消息 ID，用于请求/响应（RPC）模式中关联请求消息和响应消息。
    /// 发布方可设置，Consumer 可读取并作为回复的 CorrelationId。
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// 回复队列名称，通知消费者将响应发送到哪个队列。
    /// 在 RPC 模式中，Consumer 读取此字段后将结果发布到该队列。
    /// </summary>
    public string? ReplyTo { get; init; }

    /// <summary>
    /// 消息优先级（0-9），数值越大优先级越高。
    /// 仅在队列启用优先级的配置下生效。
    /// </summary>
    public byte? Priority { get; init; }

    /// <summary>
    /// 消息时间戳，由发布方通过 PublishOptions.Timestamp 设置，
    /// 记录消息被发布到 RabbitMQ 的时间（Unix 毫秒）。
    /// </summary>
    public global::RabbitMQ.Client.AmqpTimestamp? Timestamp { get; init; }

    /// <summary>
    /// 消息过期时间（Expiration），格式为字符串（AMQP 标准的毫秒值字符串）。
    /// 当消息在队列中驻留超过此时间后会被自动丢弃。
    /// </summary>
    public string? Expiration { get; init; }

    /// <summary>
    /// 此消息所在的 RabbitMQ Channel 实例。
    /// 用于在消息处理完成后执行 Ack/Nack 操作。
    /// </summary>
    public required global::RabbitMQ.Client.IChannel Channel { get; init; }

    /// <summary>
    /// 确认（Acknowledge）此消息，通知 RabbitMQ 该消息已被成功处理，无需重投。
    /// 对应 AMQP 的 BasicAck 命令，仅需传入 DeliveryTag。
    /// </summary>
    /// <param name="cancellationToken">取消令牌，用于异步取消确认操作。</param>
    public async ValueTask AckAsync(CancellationToken cancellationToken = default)
    {
        await Channel.BasicAckAsync(DeliveryTag, false, cancellationToken);
    }

    /// <summary>
    /// 拒绝（Negate Acknowledge）此消息，通知 RabbitMQ 该消息处理失败。
    /// 根据 requeue 参数决定是否将消息重新放回队列等待重试。
    /// </summary>
    /// <param name="requeue">true 表示重新入队（重试），false 表示丢弃或发送到死信队列。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async ValueTask NackAsync(bool requeue = true, CancellationToken cancellationToken = default)
    {
        await Channel.BasicNackAsync(DeliveryTag, false, requeue, cancellationToken);
    }
}
