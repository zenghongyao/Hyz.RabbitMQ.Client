using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// RabbitMQ 连接提供者，负责管理与 RabbitMQ Broker 的底层连接生命周期。
/// 内部持有单个 IConnection 实例，提供懒加载连接创建、自动重连和资源释放。
/// </summary>
public class RabbitMqConnectionProvider : Abstractions.IConnectionProvider
{
    private readonly Abstractions.RabbitMqConnectionOptions _options;

    /// <summary>
    /// 日志记录器，用于输出连接状态变化和异常信息。
    /// </summary>
    private readonly ILogger<RabbitMqConnectionProvider> _logger;

    /// <summary>
    /// 连接创建锁，确保同一时刻只有一个线程执行连接初始化操作（防止重复创建）。
    /// </summary>
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    /// <summary>
    /// 底层 RabbitMQ 连接实例，懒加载，仅在首次调用 GetConnectionAsync 时创建。
    /// </summary>
    private global::RabbitMQ.Client.IConnection? _connection;

    /// <summary>
    /// 当前连接状态，用于追踪连接的生命周期（Closed / Opening / Open）。
    /// </summary>
    private Abstractions.ConnectionState _state = Abstractions.ConnectionState.Closed;

    /// <summary>
    /// 延迟初始化的交换机管理器实例，用于执行 Declare / Bind / Unbind 等操作。
    /// </summary>
    private ExchangeManager? _exchangeManager;

    /// <summary>
    /// 延迟初始化的队列管理器实例，用于执行 Declare / Purge / Delete 等操作。
    /// </summary>
    private QueueManager? _queueManager;

    /// <summary>
    /// 延迟初始化的 Channel 池实例。
    /// </summary>
    private ChannelPool? _channelPool;

    /// <summary>
    /// 此连接提供者的唯一名称，来源于 RabbitMqConnectionOptions 配置。
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// 当前连接状态，反映底层 RabbitMQ 连接是否可用。
    /// </summary>
    public Abstractions.ConnectionState State => _state;

    /// <summary>
    /// 连接建立时间
    /// </summary>
    public DateTimeOffset? ConnectedAt { get; private set; }

    /// <summary>
    /// 检查连接是否健康
    /// </summary>
    public bool IsHealthy()
    {
        return _state == Abstractions.ConnectionState.Open && _connection?.IsOpen == true;
    }

    /// <summary>
    /// 获取此连接对应的交换机管理器（延迟初始化，每次调用返回同一实例）。
    /// </summary>
    public Abstractions.IExchangeManager ExchangeManager => _exchangeManager ??= new ExchangeManager(this);

    /// <summary>
    /// 获取此连接对应的队列管理器（延迟初始化，每次调用返回同一实例）。
    /// </summary>
    public Abstractions.IQueueManager QueueManager => _queueManager ??= new QueueManager(this);

    /// <summary>
    /// 获取此连接对应的 Channel 池（延迟初始化，每次调用返回同一实例）。
    /// </summary>
    public Abstractions.IChannelPool? ChannelPool => _channelPool ??= new ChannelPool(this);

    /// <summary>
    /// 获取已借出的 Channel 数量
    /// </summary>
    public int RentedChannelCount => _channelPool?.RentedCount ?? 0;

    /// <summary>
    /// 获取池中的 Channel 数量
    /// </summary>
    public int PooledChannelCount => _channelPool?.PooledCount ?? 0;

    /// <summary>
    /// 当底层 RabbitMQ 连接意外关闭时触发此事件。
    /// </summary>
    public event EventHandler<Abstractions.ConnectionEventArgs>? ConnectionShutdown;

    /// <summary>
    /// 当底层 RabbitMQ 连接自动恢复（重连）成功时触发此事件。
    /// </summary>
    public event EventHandler<Abstractions.ConnectionEventArgs>? ConnectionRecovered;

    /// <summary>
    /// 创建连接提供者实例。
    /// </summary>
    /// <param name="options">RabbitMQ 连接配置选项，包含 HostName、Port、UserName、Password 等。</param>
    /// <param name="logger">可选的日志记录器，默认为 NullLogger（无输出）。</param>
    public RabbitMqConnectionProvider(
        Abstractions.RabbitMqConnectionOptions options,
        ILogger<RabbitMqConnectionProvider>? logger = null)
    {
        _options = options;
        _logger = logger ?? NullLogger<RabbitMqConnectionProvider>.Instance;
        Name = options.GetConnectionName();
    }

    /// <summary>
    /// 同步获取底层 RabbitMQ 连接。
    /// 仅在连接已处于 Open 状态时可用；未建立连接时抛出异常。
    /// </summary>
    /// <returns>处于 Open 状态的 RabbitMQ IConnection 实例。</returns>
    /// <exception cref="InvalidOperationException">当连接未建立或已关闭时抛出。</exception>
    public global::RabbitMQ.Client.IConnection GetConnection()
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }
        throw new InvalidOperationException($"Connection '{Name}' is not open.");
    }

    /// <summary>
    /// 异步获取底层 RabbitMQ 连接。如果连接尚未建立，则先创建连接。
    /// 内部使用 SemaphoreSlim 锁确保同一时刻只有一个线程执行连接创建，避免重复初始化。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>可用的 RabbitMQ IConnection 实例。</returns>
    public async Task<global::RabbitMQ.Client.IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }
            await CreateConnectionCoreAsync(cancellationToken);
            return _connection!;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// 内部方法：创建新的 RabbitMQ 连接。
    /// 根据 RabbitMqConnectionOptions 配置 ConnectionFactory，然后调用 CreateConnectionAsync。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    private async Task CreateConnectionCoreAsync(CancellationToken cancellationToken = default)
    {
        _state = Abstractions.ConnectionState.Opening;

        // 将配置项映射到 RabbitMQ.Client 的 ConnectionFactory
        var factory = new global::RabbitMQ.Client.ConnectionFactory
        {
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = _options.AutoReconnect,
            NetworkRecoveryInterval = TimeSpan.FromMilliseconds(_options.RetryDelayMs),
            RequestedHeartbeat = TimeSpan.FromSeconds(_options.Heartbeat)
        };

        _connection = await factory.CreateConnectionAsync(cancellationToken);
        _state = Abstractions.ConnectionState.Open;
        ConnectedAt = DateTimeOffset.UtcNow;
        _logger.LogInformation("Connected to RabbitMQ: {Name} at {ConnectedAt}", Name, ConnectedAt);
    }

    /// <summary>
    /// 尝试重新建立连接。先关闭现有连接，再执行重新创建。
    /// 用于在连接异常后手动触发重连。
    /// </summary>
    /// <returns>重连成功返回 true，失败返回 false。</returns>
    public async Task<bool> TryReconnectAsync()
    {
        // 确保 Close() 的异常不会阻止锁的释放
        try { Close(); } catch { /* 忽略关闭错误 */ }

        var lockAcquired = false;
        try
        {
            await _connectionLock.WaitAsync();
            lockAcquired = true;
            await CreateConnectionCoreAsync();
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            if (lockAcquired)
            {
                _connectionLock.Release();
            }
        }
    }

    /// <summary>
    /// 同步关闭连接，释放底层 RabbitMQ IConnection 资源。
    /// </summary>
    public void Close()
    {
        if (_connection != null)
        {
            _connection.Dispose();
            _connection = null;
        }
        _state = Abstractions.ConnectionState.Closed;
        ConnectedAt = null;
    }

    /// <summary>
    /// 释放连接提供者占用的所有资源，包括底层连接和信号量。
    /// </summary>
    public void Dispose()
    {
        _channelPool?.Dispose();
        Close();
        _connectionLock.Dispose();
        GC.SuppressFinalize(this);
    }
}
