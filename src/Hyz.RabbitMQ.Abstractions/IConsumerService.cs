namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 消费服务接口
/// </summary>
public interface IConsumerService : IDisposable
{
    /// <summary>
    /// 连接名称
    /// </summary>
    string ConnectionName { get; }

    /// <summary>
    /// 开始消费消息（异步）
    /// </summary>
    Task<string> StartConsumingAsync(
        string queueName,
        IMessageHandler handler,
        ConsumerOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 停止消费
    /// </summary>
    Task StopConsumingAsync(string consumerTag);

    /// <summary>
    /// 消费消息 (IAsyncEnumerable 方式)
    /// </summary>
    IAsyncEnumerable<ReceivedMessageContext> ConsumeAsync(
        string queueName,
        ConsumerOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量消费消息（异步）
    /// </summary>
    Task<string> StartBatchConsumingAsync(
        string queueName,
        Func<IList<ReceivedMessageContext>, Task<bool>> handler,
        int batchSize = 10,
        int batchTimeoutMs = 1000,
        ConsumerOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量消费消息 (IAsyncEnumerable 方式)
    /// </summary>
    IAsyncEnumerable<IList<ReceivedMessageContext>> ConsumeBatchAsync(
        string queueName,
        int batchSize = 10,
        int batchTimeoutMs = 1000,
        ConsumerOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 确认单条消息
    /// </summary>
    ValueTask AckAsync(ReceivedMessageContext context);

    /// <summary>
    /// 拒绝单条消息
    /// </summary>
    ValueTask NackAsync(ReceivedMessageContext context, bool requeue = false);

    /// <summary>
    /// 确认当前所有待确认消息
    /// </summary>
    Task AckAllAsync();

    /// <summary>
    /// 拒绝当前所有待确认消息
    /// </summary>
    Task NackAllAsync(bool requeue = false);
}

/// <summary>
/// 多连接消费服务接口
/// </summary>
public interface IMultiConnectionConsumerService
{
    /// <summary>
    /// 获取指定连接的消费者服务
    /// </summary>
    IConsumerService GetConsumer(string connectionName = ConnectionConstants.DefaultConnectionName);

    /// <summary>
    /// 获取所有已注册的消费者服务
    /// </summary>
    IReadOnlyDictionary<string, IConsumerService> GetAllConsumers();
}
