using Abstractions = Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// RabbitMQ 交换机管理器，提供声明、删除、绑定和解绑交换机的操作。
/// 每个方法创建一个临时 Channel，执行完操作后自动释放。
/// </summary>
public class ExchangeManager : Abstractions.IExchangeManager
{
    private readonly Abstractions.IConnectionProvider _provider;

    /// <summary>
    /// 创建交换机管理器实例。
    /// </summary>
    /// <param name="provider">关联的连接提供者，用于获取 RabbitMQ 连接。</param>
    public ExchangeManager(Abstractions.IConnectionProvider provider)
    {
        _provider = provider;
    }

    /// <summary>
    /// 声明（创建或验证）一个交换机。
    /// 若交换机不存在则创建，存在则验证其属性是否匹配。
    /// </summary>
    /// <param name="exchangeName">交换机名称。</param>
    /// <param name="type">交换机类型（Direct、Fanout、Topic、Headers），默认为 Direct。</param>
    /// <param name="durable">是否持久化。true 表示交换机声明为 durable，重启后依然存在。</param>
    /// <param name="autoDelete">是否自动删除。无消费者时自动删除。</param>
    /// <param name="arguments">可选的交换机参数（如 Alternate Exchange）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>已声明的交换机信息（名称、类型、持久化属性）。</returns>
    public async Task<Abstractions.ExchangeInfo> DeclareAsync(
        string exchangeName,
        Abstractions.ExchangeType type = Abstractions.ExchangeType.Direct,
        bool durable = true,
        bool autoDelete = false,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: type.ToString().ToLower(),
            durable: durable,
            autoDelete: autoDelete,
            arguments: arguments,
            cancellationToken: cancellationToken);

        return new Abstractions.ExchangeInfo
        {
            Name = exchangeName,
            Type = type,
            Durable = durable,
            AutoDelete = autoDelete,
            Arguments = arguments
        };
    }

    /// <summary>
    /// 删除指定交换机。
    /// </summary>
    /// <param name="exchangeName">要删除的交换机名称。</param>
    /// <param name="ifUnused">true 表示仅在无队列绑定时删除，false 表示强制删除。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task DeleteAsync(
        string exchangeName,
        bool ifUnused = false,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        await channel.ExchangeDeleteAsync(exchangeName, ifUnused, noWait: false, cancellationToken);
    }

    /// <summary>
    /// 检查交换机是否存在。通过被动声明（Passive Declare）实现。
    /// </summary>
    /// <param name="exchangeName">交换机名称。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>交换机存在返回 true，不存在返回 false。</returns>
    public async Task<bool> ExistsAsync(string exchangeName, CancellationToken cancellationToken = default)
    {
        try
        {
            var connection = await _provider.GetConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
            await channel.ExchangeDeclarePassiveAsync(exchangeName, cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 将队列绑定到交换机，指定路由键。
    /// 绑定后，交换机根据路由键将消息投递给该队列。
    /// </summary>
    /// <param name="exchangeName">交换机名称。</param>
    /// <param name="queueName">队列名称。</param>
    /// <param name="routingKey">路由键模式（如 "order.created"、"user.*"）。</param>
    /// <param name="arguments">可选的绑定参数。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task BindAsync(
        string exchangeName,
        string queueName,
        string routingKey,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            arguments: arguments,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// 解除队列与交换机的绑定关系。
    /// </summary>
    /// <param name="exchangeName">交换机名称。</param>
    /// <param name="queueName">队列名称。</param>
    /// <param name="routingKey">要解除绑定的路由键（必须与原绑定时的路由键一致）。</param>
    /// <param name="arguments">可选的绑定参数。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task UnbindAsync(
        string exchangeName,
        string queueName,
        string routingKey,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default)
    {
        var connection = await _provider.GetConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(null, cancellationToken);
        await channel.QueueUnbindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            arguments: arguments,
            cancellationToken: cancellationToken);
    }
}
