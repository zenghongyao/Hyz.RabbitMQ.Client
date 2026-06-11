using RabbitMQ.Client;

namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 连接状态
/// </summary>
public enum ConnectionState
{
    /// <summary>
    /// 已关闭
    /// </summary>
    Closed,

    /// <summary>
    /// 正在打开
    /// </summary>
    Opening,

    /// <summary>
    /// 已打开
    /// </summary>
    Open,

    /// <summary>
    /// 正在关闭
    /// </summary>
    Closing,

    /// <summary>
    /// 错误
    /// </summary>
    Error
}

/// <summary>
/// 连接事件参数
/// </summary>
public class ConnectionEventArgs : EventArgs
{
    /// <summary>
    /// 连接对象
    /// </summary>
    public required IConnection Connection { get; init; }

    /// <summary>
    /// 异常信息
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>
    /// 时间戳
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// 池化 Channel 状态
/// </summary>
public enum PooledChannelState
{
    /// <summary>
    /// 空闲状态，可被借出
    /// </summary>
    Idle,

    /// <summary>
    /// 已借出
    /// </summary>
    Rented,

    /// <summary>
    /// 已释放
    /// </summary>
    Disposed
}

/// <summary>
/// Channel 池接口，用于管理 RabbitMQ Channel 的复用
/// </summary>
public interface IChannelPool : IDisposable
{
    /// <summary>
    /// 从池中获取一个 Channel
    /// </summary>
    Task<PooledChannelWrapper> RentAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 将 Channel 归还到池中
    /// </summary>
    void Return(PooledChannelWrapper pooledChannel);

    /// <summary>
    /// 将 Channel 归还到池中（异步版本）
    /// </summary>
    Task ReturnAsync(PooledChannelWrapper pooledChannel);

    /// <summary>
    /// 清理池中所有过期的 Channel
    /// </summary>
    Task CleanupExpiredChannelsAsync();

    /// <summary>
    /// 获取当前池中的 Channel 数量
    /// </summary>
    int PooledCount { get; }

    /// <summary>
    /// 获取已借出的 Channel 数量
    /// </summary>
    int RentedCount { get; }
}

/// <summary>
/// 池化 Channel 包装接口
/// </summary>
public interface PooledChannelWrapper : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Channel 状态
    /// </summary>
    PooledChannelState State { get; }

    /// <summary>
    /// 获取底层的 RabbitMQ Channel
    /// </summary>
    IChannel Channel { get; }

    /// <summary>
    /// 检查 Channel 是否健康
    /// </summary>
    bool IsHealthy();

    /// <summary>
    /// 检查 Channel 是否已过期
    /// </summary>
    bool IsExpired();

    /// <summary>
    /// 标记为已借出
    /// </summary>
    void MarkRented();

    /// <summary>
    /// 标记为已归还
    /// </summary>
    void MarkReturned();

    /// <summary>
    /// 标记为已释放
    /// </summary>
    void MarkDisposed();
}

/// <summary>
/// 连接提供者接口
/// </summary>
public interface IConnectionProvider : IDisposable
{
    /// <summary>
    /// 连接名称
    /// </summary>
    string Name { get; }

    /// <summary>
    /// 获取当前连接状态
    /// </summary>
    ConnectionState State { get; }

    /// <summary>
    /// 获取可用连接
    /// </summary>
    IConnection GetConnection();

    /// <summary>
    /// 异步获取可用连接
    /// </summary>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 检查连接是否健康
    /// </summary>
    bool IsHealthy();

    /// <summary>
    /// 获取 Channel 池（如果支持）
    /// </summary>
    IChannelPool? ChannelPool { get; }

    /// <summary>
    /// 获取已借出的 Channel 数量
    /// </summary>
    int RentedChannelCount { get; }

    /// <summary>
    /// 获取池中的 Channel 数量
    /// </summary>
    int PooledChannelCount { get; }

    /// <summary>
    /// 获取连接建立时间
    /// </summary>
    DateTimeOffset? ConnectedAt { get; }

    /// <summary>
    /// 尝试重连
    /// </summary>
    Task<bool> TryReconnectAsync();

    /// <summary>
    /// 连接断开事件
    /// </summary>
    event EventHandler<ConnectionEventArgs>? ConnectionShutdown;

    /// <summary>
    /// 连接恢复事件
    /// </summary>
    event EventHandler<ConnectionEventArgs>? ConnectionRecovered;

    /// <summary>
    /// 交换机管理器
    /// </summary>
    IExchangeManager ExchangeManager { get; }

    /// <summary>
    /// 队列管理器
    /// </summary>
    IQueueManager QueueManager { get; }
}
