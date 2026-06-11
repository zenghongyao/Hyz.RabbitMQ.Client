using System.Diagnostics;
using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// RabbitMQ 发布服务，提供向队列或交换机发布消息的 API。
/// 支持单条发布、带确认的发布和批量发布，自动管理 Channel 的创建与复用。
/// </summary>
public class PublisherService : Abstractions.IPublisherService
{
    private readonly Abstractions.IConnectionProvider _connectionProvider;
    private readonly ILogger<PublisherService> _logger;
    private readonly ChannelPool? _channelPool;

    /// <summary>
    /// 当前绑定的连接名称，来自底层 IConnectionProvider。
    /// </summary>
    public string ConnectionName => _connectionProvider.Name;

    /// <summary>
    /// 创建发布服务实例。
    /// </summary>
    public PublisherService(
        Abstractions.IConnectionProvider connectionProvider,
        ILogger<PublisherService>? logger = null)
    {
        _connectionProvider = connectionProvider;
        _logger = logger ?? NullLogger<PublisherService>.Instance;

        // 如果连接提供者支持 Channel 池，则使用池化
        if (_connectionProvider.ChannelPool is ChannelPool pool)
        {
            _channelPool = pool;
        }
    }

    /// <summary>
    /// 向指定队列发布一条消息。
    /// </summary>
    public Task PublishAsync(
        string queueName,
        Abstractions.MessageBody message,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return PublishToExchangeAsync(string.Empty, queueName, message, options, cancellationToken);
    }

    /// <summary>
    /// 向指定交换机发布消息。
    /// </summary>
    public async Task PublishToExchangeAsync(
        string exchangeName,
        string routingKey,
        Abstractions.MessageBody message,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using var channelWrapper = await RentChannelAsync(cancellationToken);

        var props = new global::RabbitMQ.Client.BasicProperties
        {
            Persistent = options?.Persistent ?? true,
            ContentType = options?.ContentType ?? "application/json",
            MessageId = options?.MessageId ?? Guid.NewGuid().ToString()
        };

        if (options?.CorrelationId != null) props.CorrelationId = options.CorrelationId;
        if (options?.ReplyTo != null) props.ReplyTo = options.ReplyTo;
        if (options?.Headers != null) props.Headers = options.Headers;

        await channelWrapper.Channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: routingKey,
            mandatory: options?.Mandatory ?? false,
            basicProperties: props,
            body: message.Bytes,
            cancellationToken: cancellationToken);

        _logger.LogDebug("Published message to {Exchange}/{RoutingKey}", exchangeName, routingKey);
    }

    /// <summary>
    /// 向交换机发布消息并等待确认。
    /// </summary>
    public async Task<Abstractions.PublishResult> PublishWithConfirmationAsync(
        string exchangeName,
        string routingKey,
        Abstractions.MessageBody message,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await PublishToExchangeAsync(exchangeName, routingKey, message, options, cancellationToken);
            return Abstractions.PublishResult.Success(0);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Publish failed");
            return Abstractions.PublishResult.Failure(ex.Message);
        }
    }

    /// <summary>
    /// 批量向同一个交换机/路由键发布多条消息。
    /// </summary>
    public async Task<Abstractions.BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        string routingKey,
        IEnumerable<Abstractions.MessageBody> messages,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var failures = new List<Abstractions.BatchPublishFailure>();
        var successCount = 0;
        var index = 0;

        await using var channelWrapper = await RentChannelAsync(cancellationToken);

        var props = new global::RabbitMQ.Client.BasicProperties
        {
            Persistent = options?.Persistent ?? true,
            ContentType = options?.ContentType ?? "application/json"
        };

        foreach (var message in messages)
        {
            try
            {
                await channelWrapper.Channel.BasicPublishAsync(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    mandatory: false,
                    basicProperties: props,
                    body: message.Bytes,
                    cancellationToken: cancellationToken);
                successCount++;
            }
            catch (Exception ex)
            {
                failures.Add(new Abstractions.BatchPublishFailure(index, routingKey, ex));
            }
            index++;
        }

        stopwatch.Stop();

        return new Abstractions.BatchPublishResult
        {
            SuccessCount = successCount,
            FailedCount = failures.Count,
            Failures = failures,
            Elapsed = stopwatch.Elapsed
        };
    }

    /// <summary>
    /// 批量向同一个交换机发布消息，但每组消息可使用不同的路由键。
    /// </summary>
    public async Task<Abstractions.BatchPublishResult> PublishBatchAsync(
        string exchangeName,
        IDictionary<string, IEnumerable<Abstractions.MessageBody>> messages,
        Abstractions.PublishOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var allFailures = new List<Abstractions.BatchPublishFailure>();
        var totalSuccess = 0;
        var index = 0;

        foreach (var kvp in messages)
        {
            var routingKey = kvp.Key;
            var messageList = kvp.Value;
            var result = await PublishBatchAsync(exchangeName, routingKey, messageList, options, cancellationToken);
            totalSuccess += result.SuccessCount;

            foreach (var failure in result.Failures)
            {
                allFailures.Add(new Abstractions.BatchPublishFailure(failure.Index + index, routingKey, failure.Exception));
            }
            index += result.SuccessCount + result.FailedCount;
        }

        stopwatch.Stop();

        return new Abstractions.BatchPublishResult
        {
            SuccessCount = totalSuccess,
            FailedCount = allFailures.Count,
            Failures = allFailures,
            Elapsed = stopwatch.Elapsed
        };
    }

    /// <summary>
    /// 从池中借出 Channel，封装为可释放对象
    /// </summary>
    private async Task<PooledChannelWrapper> RentChannelAsync(CancellationToken cancellationToken)
    {
        if (_channelPool != null)
        {
            var pooledChannel = await _channelPool.RentAsync(cancellationToken);
            return new PooledChannelWrapper(pooledChannel, _channelPool);
        }

        // 不使用池时，直接创建 Channel
        var connection = await _connectionProvider.GetConnectionAsync(cancellationToken);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        return new PooledChannelWrapper(channel);
    }
}

/// <summary>
/// Channel 包装器，确保使用后正确归还
/// </summary>
internal sealed class PooledChannelWrapper : IAsyncDisposable
{
    private readonly Abstractions.PooledChannelWrapper? _pooledChannel;
    private readonly Abstractions.IChannelPool? _pool;
    private readonly IChannel? _channel;
    private readonly bool _ownsChannel;

    public PooledChannelWrapper(Abstractions.PooledChannelWrapper pooledChannel, Abstractions.IChannelPool pool)
    {
        _pooledChannel = pooledChannel;
        _pool = pool;
        _channel = pooledChannel.Channel;
        _ownsChannel = true;
    }

    public PooledChannelWrapper(IChannel channel)
    {
        _channel = channel;
        _ownsChannel = true;
    }

    public IChannel Channel => _channel!;

    public async ValueTask DisposeAsync()
    {
        if (_ownsChannel)
        {
            if (_pooledChannel != null && _pool != null)
            {
                await _pool.ReturnAsync(_pooledChannel);
            }
            else if (_channel != null)
            {
                await _channel.CloseAsync();
                _channel.Dispose();
            }
        }
    }
}
