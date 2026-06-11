namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 发布服务接口
/// </summary>
public interface IPublisherService
{
    /// <summary>
    /// 连接名称
    /// </summary>
    string ConnectionName { get; }

    /// <summary>
    /// 发布消息到指定队列
    /// </summary>
    Task PublishAsync(
        string queueName,
        MessageBody message,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 发布消息到交换机
    /// </summary>
    Task PublishToExchangeAsync(
        string exchangeName,
        string routingKey,
        MessageBody message,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 发布消息并等待确认
    /// </summary>
    Task<PublishResult> PublishWithConfirmationAsync(
        string exchangeName,
        string routingKey,
        MessageBody message,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量发布消息
    /// </summary>
    Task<BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        string routingKey,
        IEnumerable<MessageBody> messages,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量发布消息到多个路由键
    /// </summary>
    Task<BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        IDictionary<string, IEnumerable<MessageBody>> messages,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 多连接发布服务接口
/// </summary>
public interface IMultiConnectionPublisherService
{
    /// <summary>
    /// 发布到指定连接
    /// </summary>
    Task PublishAsync(
        string queueName,
        MessageBody message,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 发布到指定连接的交换机
    /// </summary>
    Task PublishToExchangeAsync(
        string exchangeName,
        string routingKey,
        MessageBody message,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量发布到指定连接
    /// </summary>
    Task<BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        string routingKey,
        IEnumerable<MessageBody> messages,
        string connectionName = ConnectionConstants.DefaultConnectionName,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取指定连接的发布服务
    /// </summary>
    IPublisherService GetPublisher(string connectionName = ConnectionConstants.DefaultConnectionName);
}
