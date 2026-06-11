namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 消息处理器接口
/// </summary>
public interface IMessageHandler
{
    /// <summary>
    /// 处理接收到的消息
    /// </summary>
    Task<HandleResult> HandleAsync(ReceivedMessageContext context, CancellationToken cancellationToken = default);
}

/// <summary>
/// 批量消息处理器接口
/// </summary>
public interface IBatchMessageHandler
{
    /// <summary>
    /// 批量处理消息
    /// </summary>
    Task<BatchHandleResult> HandleBatchAsync(
        IReadOnlyList<ReceivedMessageContext> messages,
        CancellationToken cancellationToken = default);
}
