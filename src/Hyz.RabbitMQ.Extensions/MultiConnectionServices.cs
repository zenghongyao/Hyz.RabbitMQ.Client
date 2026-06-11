using Hyz.RabbitMQ.Abstractions;
using System.Collections.Concurrent;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// 多连接发布服务，支持通过指定连接名称向不同 RabbitMQ Broker 发布消息。
/// 内部使用 ConcurrentDictionary 缓存每个连接的 IPublisherService 实例，实现按需懒创建和复用。
/// </summary>
public class MultiConnectionPublisherService : Abstractions.IMultiConnectionPublisherService
{
    private readonly IConnectionManager _connectionManager;

    /// <summary>
    /// 缓存各连接的 PublisherService 实例，避免重复创建。
    /// </summary>
    private readonly ConcurrentDictionary<string, Abstractions.IPublisherService> _publishers = new();

    /// <summary>
    /// 创建多连接发布服务实例。
    /// </summary>
    /// <param name="connectionManager">连接管理器，用于按名称获取 IConnectionProvider。</param>
    public MultiConnectionPublisherService(IConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
    }

    /// <summary>
    /// 向指定连接的队列发布消息。
    /// </summary>
    /// <param name="queueName">目标队列名称。</param>
    /// <param name="message">消息体内容。</param>
    /// <param name="connectionName">目标连接名称，默认为 "Default"。</param>
    /// <param name="options">可选的发布配置。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task PublishAsync(
        string queueName,
        Abstractions.MessageBody message,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var publisher = GetPublisher(connectionName);
        await publisher.PublishAsync(queueName, message, options, cancellationToken);
    }

    /// <summary>
    /// 向指定连接的交换机发布消息。
    /// </summary>
    /// <param name="exchangeName">目标交换机名称。</param>
    /// <param name="routingKey">路由键。</param>
    /// <param name="message">消息体内容。</param>
    /// <param name="connectionName">目标连接名称。</param>
    /// <param name="options">可选的发布配置。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task PublishToExchangeAsync(
        string exchangeName,
        string routingKey,
        Abstractions.MessageBody message,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var publisher = GetPublisher(connectionName);
        await publisher.PublishToExchangeAsync(exchangeName, routingKey, message, options, cancellationToken);
    }

    /// <summary>
    /// 向指定连接的交换机批量发布消息。
    /// </summary>
    /// <param name="exchangeName">目标交换机名称。</param>
    /// <param name="routingKey">路由键。</param>
    /// <param name="messages">消息集合。</param>
    /// <param name="connectionName">目标连接名称。</param>
    /// <param name="options">可选的发布配置。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>批量发布结果。</returns>
    public async Task<Abstractions.BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        string routingKey,
        IEnumerable<Abstractions.MessageBody> messages,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var publisher = GetPublisher(connectionName);
        return await publisher.PublishBatchAsync(exchangeName, routingKey, messages, options, cancellationToken);
    }

    /// <summary>
    /// 获取指定连接的发布服务实例。
    /// 首次调用时通过 GetOrAdd 懒创建，后续返回缓存实例。
    /// </summary>
    /// <param name="connectionName">连接名称。</param>
    /// <returns>对应连接的 IPublisherService 实例。</returns>
    public Abstractions.IPublisherService GetPublisher(string connectionName = ConnectionConstants.DefaultConnectionName)
    {
        return _publishers.GetOrAdd(connectionName, name =>
        {
            var provider = _connectionManager.GetProvider(name);
            return new Core.PublisherService(provider);
        });
    }
}

/// <summary>
/// 多连接消费服务，支持通过指定连接名称从不同 RabbitMQ Broker 消费消息。
/// 内部同样使用 ConcurrentDictionary 缓存各连接的 IConsumerService 实例。
/// </summary>
public class MultiConnectionConsumerService : Abstractions.IMultiConnectionConsumerService
{
    private readonly IConnectionManager _connectionManager;

    /// <summary>
    /// 缓存各连接的 ConsumerService 实例。
    /// </summary>
    private readonly ConcurrentDictionary<string, Abstractions.IConsumerService> _consumers = new();

    /// <summary>
    /// 创建多连接消费服务实例。
    /// </summary>
    /// <param name="connectionManager">连接管理器。</param>
    public MultiConnectionConsumerService(IConnectionManager connectionManager)
    {
        _connectionManager = connectionManager;
    }

    /// <summary>
    /// 获取指定连接的消费服务实例。首次调用时懒创建。
    /// </summary>
    /// <param name="connectionName">连接名称，默认为 "Default"。</param>
    /// <returns>对应连接的 IConsumerService 实例。</returns>
    public Abstractions.IConsumerService GetConsumer(string connectionName = ConnectionConstants.DefaultConnectionName)
    {
        return _consumers.GetOrAdd(connectionName, name =>
        {
            var provider = _connectionManager.GetProvider(name);
            return new Core.ConsumerService(provider);
        });
    }

    /// <summary>
    /// 获取所有已注册连接的消费服务实例字典。
    /// </summary>
    /// <returns>连接名称到 IConsumerService 实例的只读字典。</returns>
    public IReadOnlyDictionary<string, Abstractions.IConsumerService> GetAllConsumers()
    {
        var result = new Dictionary<string, Abstractions.IConsumerService>();
        foreach (var name in _connectionManager.GetAllConnectionNames())
        {
            result[name] = GetConsumer(name);
        }
        return result;
    }
}
