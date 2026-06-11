using Abstractions = Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// RabbitMQ 队列管理器，提供队列的声明、删除、清空、查询等操作。
/// 每个方法创建一个临时 Channel，执行完操作后自动通过 await using 释放。
/// </summary>
public class QueueManager : Abstractions.IQueueManager
{
    private readonly Abstractions.IConnectionProvider _provider;

    /// <summary>
    /// 创建队列管理器实例。
    /// </summary>
    /// <param name="provider">关联的连接提供者。</param>
    public QueueManager(Abstractions.IConnectionProvider provider)
    {
        _provider = provider;
    }

    /// <summary>
    /// 声明（创建或验证）一个队列。
    /// 若队列不存在则创建，存在则返回其当前的消息数量和消费者数量。
    /// </summary>
    /// <param name="queueName">队列名称。</param>
    /// <param name="durable">是否持久化。true 表示队列会在 Broker 重启后保留。</param>
    /// <param name="exclusive">是否独占。true 表示仅允许当前连接使用此队列。</param>
    /// <param name="autoDelete">是否自动删除。true 表示最后一个消费者取消订阅后队列自动删除。</param>
    /// <param name="arguments">可选的队列参数，如 x-message-ttl（消息过期时间）、x-max-length（队列最大长度）等。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>队列信息，包含队列名称、当前消息数量和消费者数量。</returns>
    public async Task<Abstractions.QueueInfo> DeclareAsync(
        string queueName,
        bool durable = true,
        bool exclusive = false,
        bool autoDelete = false,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);

        var result = await channel.QueueDeclareAsync(
            queue: queueName,
            durable: durable,
            exclusive: exclusive,
            autoDelete: autoDelete,
            arguments: arguments,
            cancellationToken: cancellationToken);

        return new Abstractions.QueueInfo
        {
            Name = result.QueueName,
            MessageCount = result.MessageCount,
            ConsumerCount = result.ConsumerCount
        };
    }

    /// <summary>
    /// 删除指定队列及其所有消息。
    /// </summary>
    /// <param name="queueName">队列名称。</param>
    /// <param name="ifUnused">true 表示仅在无消费者时删除，false 表示强制删除。</param>
    /// <param name="ifEmpty">true 表示仅在队列为空时删除，false 表示无论是否为空都删除。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>被删除的消息数量。</returns>
    public async Task<uint> DeleteAsync(
        string queueName,
        bool ifUnused = false,
        bool ifEmpty = false,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        return await channel.QueueDeleteAsync(queueName, ifUnused, ifEmpty, noWait: false, cancellationToken);
    }

    /// <summary>
    /// 清空队列中的所有消息，但不删除队列本身。
    /// </summary>
    /// <param name="queueName">队列名称。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>被清空的消息数量。</returns>
    public async Task<uint> PurgeAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        return await channel.QueuePurgeAsync(queueName, cancellationToken);
    }

    /// <summary>
    /// 检查队列是否存在。通过被动声明（Passive Declare）实现。
    /// </summary>
    /// <param name="queueName">队列名称。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>队列存在返回 true，否则返回 false。</returns>
    public async Task<bool> ExistsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        try
        {
            var connection = await _provider.GetConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
            await channel.QueueDeclarePassiveAsync(queueName, cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 获取队列的详细信息（消息数量和消费者数量）。
    /// </summary>
    /// <param name="queueName">队列名称。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>队列信息。</returns>
    public async Task<Abstractions.QueueInfo> GetInfoAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        var result = await channel.QueueDeclarePassiveAsync(queueName, cancellationToken);
        return new Abstractions.QueueInfo
        {
            Name = result.QueueName,
            MessageCount = result.MessageCount,
            ConsumerCount = result.ConsumerCount
        };
    }
}
