namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 交换机管理器接口
/// </summary>
public interface IExchangeManager
{
    /// <summary>
    /// 声明交换机
    /// </summary>
    Task<ExchangeInfo> DeclareAsync(
        string exchangeName,
        ExchangeType type = ExchangeType.Direct,
        bool durable = true,
        bool autoDelete = false,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 删除交换机
    /// </summary>
    Task DeleteAsync(
        string exchangeName,
        bool ifUnused = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 检查交换机是否存在
    /// </summary>
    Task<bool> ExistsAsync(string exchangeName, CancellationToken cancellationToken = default);

    /// <summary>
    /// 绑定交换机到队列
    /// </summary>
    Task BindAsync(
        string exchangeName,
        string queueName,
        string routingKey,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 解绑交换机与队列
    /// </summary>
    Task UnbindAsync(
        string exchangeName,
        string queueName,
        string routingKey,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 队列管理器接口
/// </summary>
public interface IQueueManager
{
    /// <summary>
    /// 声明队列
    /// </summary>
    Task<QueueInfo> DeclareAsync(
        string queueName,
        bool durable = true,
        bool exclusive = false,
        bool autoDelete = false,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 删除队列
    /// </summary>
    Task<uint> DeleteAsync(
        string queueName,
        bool ifUnused = false,
        bool ifEmpty = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 清空队列
    /// </summary>
    Task<uint> PurgeAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// 检查队列是否存在
    /// </summary>
    Task<bool> ExistsAsync(string queueName, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取队列信息
    /// </summary>
    Task<QueueInfo> GetInfoAsync(string queueName, CancellationToken cancellationToken = default);
}
