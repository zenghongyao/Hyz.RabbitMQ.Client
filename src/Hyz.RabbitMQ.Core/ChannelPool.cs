using System.Collections.Concurrent;
using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// Channel 池，用于复用 RabbitMQ Channel 以减少创建/销毁开销。
/// 支持借出和归还操作，自动管理 Channel 的生命周期。
/// </summary>
public class ChannelPool : Abstractions.IChannelPool, IDisposable
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly ILogger<ChannelPool> _logger;
    private readonly ConcurrentBag<PooledChannel> _channels = new();
    private readonly SemaphoreSlim _semaphore;
    private readonly int _maxPoolSize;
    private readonly int _maxChannelLifetimeMinutes;
    private bool _disposed;

    /// <summary>
    /// 创建 Channel 池
    /// </summary>
    /// <param name="connectionProvider">底层连接提供者</param>
    /// <param name="maxPoolSize">最大池大小，默认 20</param>
    /// <param name="maxChannelLifetimeMinutes">Channel 最大存活时间（分钟），默认 10 分钟回收</param>
    /// <param name="logger">日志记录器</param>
    public ChannelPool(
        IConnectionProvider connectionProvider,
        int maxPoolSize = 20,
        int maxChannelLifetimeMinutes = 10,
        ILogger<ChannelPool>? logger = null)
    {
        _connectionProvider = connectionProvider;
        _maxPoolSize = maxPoolSize;
        _maxChannelLifetimeMinutes = maxChannelLifetimeMinutes;
        _logger = logger ?? NullLogger<ChannelPool>.Instance;
        _semaphore = new SemaphoreSlim(_maxPoolSize, _maxPoolSize);
    }

    /// <summary>
    /// 从池中获取一个 Channel
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>可用的 Channel</returns>
    public async Task<Abstractions.PooledChannelWrapper> RentAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ChannelPool));

        await _semaphore.WaitAsync(cancellationToken);

        try
        {
            // 尝试从池中获取可用的 Channel
            while (_channels.TryTake(out var pooledChannel))
            {
                if (pooledChannel.IsHealthy())
                {
                    pooledChannel.MarkRented();
                    _logger.LogDebug("Reused pooled channel (Age: {Age}s)", pooledChannel.GetAgeSeconds());
                    return pooledChannel;
                }

                // Channel 不健康，释放它
                await pooledChannel.DisposeAsync();
                _logger.LogDebug("Disposed unhealthy channel");
            }

            // 池为空，创建新的 Channel
            var connection = await _connectionProvider.GetConnectionAsync(cancellationToken);
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            var newPooledChannel = new PooledChannel(channel, _maxChannelLifetimeMinutes, _logger);

            _logger.LogDebug("Created new channel for pool");
            return newPooledChannel;
        }
        catch
        {
            _semaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// 将 Channel 归还到池中（同步版本）
    /// </summary>
    /// <param name="pooledChannel">要归还的 Channel</param>
    public void Return(Abstractions.PooledChannelWrapper pooledChannel)
    {
        if (_disposed)
        {
            pooledChannel.MarkDisposed();
            pooledChannel.Dispose();
            return;
        }

        if (pooledChannel is PooledChannel channel)
        {
            if (channel.IsHealthy())
            {
                channel.MarkReturned();
                _channels.Add(channel);
                _logger.LogDebug("Returned channel to pool");
            }
            else
            {
                channel.MarkDisposed();
                channel.Dispose();
                _logger.LogDebug("Disposed unhealthy channel instead of returning to pool");
            }
        }

        _semaphore.Release();
    }

    /// <summary>
    /// 将 Channel 归还到池中（异步版本）
    /// </summary>
    /// <param name="pooledChannel">要归还的 Channel</param>
    public async Task ReturnAsync(Abstractions.PooledChannelWrapper pooledChannel)
    {
        if (_disposed)
        {
            pooledChannel.MarkDisposed();
            await pooledChannel.DisposeAsync();
            return;
        }

        if (pooledChannel is PooledChannel channel)
        {
            if (channel.IsHealthy())
            {
                channel.MarkReturned();
                _channels.Add(channel);
                _logger.LogDebug("Returned channel to pool");
            }
            else
            {
                channel.MarkDisposed();
                await channel.DisposeAsync();
                _logger.LogDebug("Disposed unhealthy channel instead of returning to pool");
            }
        }

        _semaphore.Release();
    }

    /// <summary>
    /// 清理池中所有过期的 Channel
    /// 使用快照方式避免并发归还时丢失 Channel
    /// </summary>
    public async Task CleanupExpiredChannelsAsync()
    {
        // 先快照当前所有 Channel，避免清理过程中丢失其他线程归还的 Channel
        var allChannels = new List<PooledChannel>();
        while (_channels.TryTake(out var pooledChannel))
        {
            allChannels.Add(pooledChannel);
        }

        // 分离过期和非过期的 Channel
        var expired = new List<PooledChannel>();
        foreach (var channel in allChannels)
        {
            if (channel.IsExpired())
            {
                expired.Add(channel);
            }
            else
            {
                // 仍然健康的 Channel 归还到池中
                _channels.Add(channel);
            }
        }

        // 异步销毁过期的 Channel
        foreach (var channel in expired)
        {
            await channel.DisposeAsync();
            _logger.LogDebug("Cleaned up expired channel");
        }
    }

    /// <summary>
    /// 获取当前池中的 Channel 数量
    /// </summary>
    public int PooledCount => _channels.Count;

    /// <summary>
    /// 获取已借出的 Channel 数量
    /// </summary>
    public int RentedCount => _maxPoolSize - _semaphore.CurrentCount;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        while (_channels.TryTake(out var pooledChannel))
        {
            pooledChannel.Dispose();
        }

        _semaphore.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// 包装的 Channel 对象，包含元数据用于池化管理
/// </summary>
public class PooledChannel : Abstractions.PooledChannelWrapper
{
    private readonly IChannel _channel;
    private readonly int _maxLifetimeMinutes;
    private readonly ILogger _logger;
    private readonly DateTime _createdAt;
    private Abstractions.PooledChannelState _state = Abstractions.PooledChannelState.Idle;
    private DateTime _lastUsedAt;
    private bool _disposed;

    public PooledChannel(IChannel channel, int maxLifetimeMinutes, ILogger logger)
    {
        _channel = channel;
        _maxLifetimeMinutes = maxLifetimeMinutes;
        _logger = logger;
        _createdAt = DateTime.UtcNow;
        _lastUsedAt = _createdAt;
    }

    /// <summary>
    /// 获取底层的 RabbitMQ Channel
    /// </summary>
    public IChannel Channel => _channel;

    /// <summary>
    /// 获取当前状态
    /// </summary>
    public Abstractions.PooledChannelState State => _state;

    /// <summary>
    /// 检查 Channel 是否健康（未关闭且未过期）
    /// </summary>
    public bool IsHealthy()
    {
        if (_state == Abstractions.PooledChannelState.Disposed || _disposed) return false;
        try
        {
            return _channel.IsOpen && !IsExpired();
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 检查 Channel 是否已过期
    /// </summary>
    public bool IsExpired()
    {
        return (DateTime.UtcNow - _createdAt).TotalMinutes >= _maxLifetimeMinutes;
    }

    /// <summary>
    /// 获取 Channel 已创建的时长（秒）
    /// </summary>
    public double GetAgeSeconds()
    {
        return (DateTime.UtcNow - _createdAt).TotalSeconds;
    }

    public void MarkRented()
    {
        _state = Abstractions.PooledChannelState.Rented;
        _lastUsedAt = DateTime.UtcNow;
    }

    public void MarkReturned()
    {
        _state = Abstractions.PooledChannelState.Idle;
    }

    public void MarkDisposed()
    {
        _state = Abstractions.PooledChannelState.Disposed;
    }

    /// <summary>
    /// 同步释放 Channel（委托给异步版本以避免线程阻塞）
    /// </summary>
    public void Dispose()
    {
        if (_state == Abstractions.PooledChannelState.Disposed || _disposed) return;
        _disposed = true;

        try
        {
            if (_channel.IsOpen)
            {
                // 使用异步关闭避免线程阻塞，但等待最多5秒
                using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try
                {
                    _channel.CloseAsync().Wait(timeoutCts.Token);
                }
                catch (OperationCanceledException)
                {
                    // 超时忽略，继续关闭
                }
            }
            _channel.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing pooled channel");
        }

        _state = Abstractions.PooledChannelState.Disposed;
    }

    /// <summary>
    /// 异步释放 Channel
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_state == Abstractions.PooledChannelState.Disposed || _disposed) return;
        _disposed = true;

        try
        {
            if (_channel.IsOpen)
            {
                await _channel.CloseAsync();
            }
            _channel.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error disposing pooled channel");
        }

        _state = Abstractions.PooledChannelState.Disposed;
    }
}
