using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// RabbitMQ 消费服务，提供从队列消费消息的多种模式：
/// 事件驱动（StartConsumingAsync）、异步流（ConsumeAsync）、批量消费（StartBatchConsumingAsync / ConsumeBatchAsync）。
/// 内部维护所有已启动的消费者标签（ConsumerTag），通过 IDisposable 统一释放资源。
/// </summary>
public class ConsumerService : Abstractions.IConsumerService
{
    private readonly Abstractions.IConnectionProvider _connectionProvider;
    private readonly ILogger<ConsumerService> _logger;

    /// <summary>
    /// 存储所有已启动的消费者标签与对应 Channel、队列名的映射。
    /// 用于 StopConsumingAsync 和 Dispose 时关闭消费者。
    /// </summary>
    private readonly ConcurrentDictionary<string, (global::RabbitMQ.Client.IChannel Channel, string QueueName)> _consumerTags = new();

    /// <summary>
    /// 存储批量消费者的取消令牌源，用于优雅停止后台任务。
    /// </summary>
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _batchConsumerCts = new();

    /// <summary>
    /// 当前绑定的连接名称。
    /// </summary>
    public string ConnectionName => _connectionProvider.Name;

    /// <summary>
    /// 创建消费服务实例。
    /// </summary>
    /// <param name="connectionProvider">连接提供者。</param>
    /// <param name="logger">可选的日志记录器。</param>
    public ConsumerService(
        Abstractions.IConnectionProvider connectionProvider,
        ILogger<ConsumerService>? logger = null)
    {
        _connectionProvider = connectionProvider;
        _logger = logger ?? NullLogger<ConsumerService>.Instance;
    }

    /// <summary>
    /// 启动基于事件回调的消费模式。
    /// 使用 AsyncEventingBasicConsumer，收到消息后调用用户提供的 handler 处理，
    /// 并根据 handler 返回的 HandleResult 自动执行 Ack / Nack / Retry。
    /// </summary>
    public async Task<string> StartConsumingAsync(
        string queueName,
        Abstractions.IMessageHandler handler,
        Abstractions.ConsumerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var channel = await GetChannelAsync();
        var consumerTag = options?.ConsumerTag ?? Guid.NewGuid().ToString("N");

        await channel.BasicQosAsync(0, options?.PrefetchCount ?? 10, false);

        var consumer = new global::RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            try
            {
                var context = new Abstractions.ReceivedMessageContext
                {
                    Body = ea.Body.ToArray(),
                    RoutingKey = ea.RoutingKey ?? string.Empty,
                    ExchangeName = ea.Exchange ?? string.Empty,
                    QueueName = queueName,
                    DeliveryTag = ea.DeliveryTag,
                    MessageId = ea.BasicProperties.MessageId,
                    ContentType = ea.BasicProperties.ContentType,
                    Headers = ea.BasicProperties.Headers,
                    Redelivered = ea.Redelivered,
                    CorrelationId = ea.BasicProperties.CorrelationId,
                    ReplyTo = ea.BasicProperties.ReplyTo,
                    Priority = ea.BasicProperties.Priority,
                    Timestamp = ea.BasicProperties.Timestamp,
                    Expiration = ea.BasicProperties.Expiration,
                    Channel = channel
                };

                var result = await handler.HandleAsync(context, cancellationToken);

                if (result.IsSuccess)
                    await channel.BasicAckAsync(ea.DeliveryTag, false, cancellationToken);
                else if (result.Requeue)
                    await channel.BasicNackAsync(ea.DeliveryTag, false, true, cancellationToken);
                else
                    await channel.BasicNackAsync(ea.DeliveryTag, false, false, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error handling message");
                await channel.BasicNackAsync(ea.DeliveryTag, false, false, cancellationToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: options?.AutoAck ?? false,
            consumerTag: consumerTag,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer);

        _consumerTags.TryAdd(consumerTag, (channel, queueName));
        _logger.LogInformation("Started consuming: {Queue}", queueName);

        return consumerTag;
    }

    /// <summary>
    /// 停止指定消费者的消费，关闭对应的 Channel 和后台任务。
    /// </summary>
    public async Task StopConsumingAsync(string consumerTag)
    {
        if (_consumerTags.TryRemove(consumerTag, out var info))
        {
            // 取消批量消费者的后台任务
            if (_batchConsumerCts.TryRemove(consumerTag, out var cts))
            {
                #if NETSTANDARD2_0
                cts.Cancel();
#else
                await cts.CancelAsync();
#endif
                cts.Dispose();
            }

            await info.Channel.DisposeAsync();
            _logger.LogInformation("Stopped consuming: {Tag}", consumerTag);
        }
    }

    /// <summary>
    /// 启动基于异步流（IAsyncEnumerable）的消费模式。
    /// 使用 Channel<T> 实现无轮询的异步消息传递，高效且低延迟。
    /// </summary>
    public async IAsyncEnumerable<Abstractions.ReceivedMessageContext> ConsumeAsync(
        string queueName,
        Abstractions.ConsumerOptions? options = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = await GetChannelAsync().ConfigureAwait(false);

        // 使用 Channel 实现高效的无锁队列，避免轮询
        var messageChannel = System.Threading.Channels.Channel.CreateBounded<Abstractions.ReceivedMessageContext>(
            new System.Threading.Channels.BoundedChannelOptions(options?.PrefetchCount ?? 10)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
            });

        var consumer = new global::RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (sender, ea) =>
        {
            var context = new Abstractions.ReceivedMessageContext
            {
                Body = ea.Body.ToArray(),
                RoutingKey = ea.RoutingKey ?? string.Empty,
                ExchangeName = ea.Exchange ?? string.Empty,
                QueueName = queueName,
                DeliveryTag = ea.DeliveryTag,
                Channel = channel
            };
            messageChannel.Writer.TryWrite(context);
            return Task.CompletedTask;
        };

        await channel.BasicQosAsync(0, options?.PrefetchCount ?? 10, false).ConfigureAwait(false);
        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer).ConfigureAwait(false);

        try
        {
#if NETSTANDARD2_0
            while (await messageChannel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (messageChannel.Reader.TryRead(out var message))
                {
                    yield return message;
                }
            }
#else
            await foreach (var message in messageChannel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return message;
            }
#endif
        }
        finally
        {
            messageChannel.Writer.Complete();
            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 启动基于事件回调的批量消费模式。
    /// </summary>
    public async Task<string> StartBatchConsumingAsync(
        string queueName,
        Func<IList<Abstractions.ReceivedMessageContext>, Task<bool>> handler,
        int batchSize = 10,
        int batchTimeoutMs = 1000,
        Abstractions.ConsumerOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var channel = await GetChannelAsync();
        var consumerTag = options?.ConsumerTag ?? Guid.NewGuid().ToString("N");
        var batch = new List<Abstractions.ReceivedMessageContext>();
        var batchLock = new object();
        var lastBatchTime = DateTime.UtcNow;
        using var timerCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timerCts.Token);

        // 保存取消令牌源，用于优雅停止
        _batchConsumerCts[consumerTag] = linkedCts;

        await channel.BasicQosAsync(0, options?.PrefetchCount ?? 10, false);

        var consumer = new global::RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            var context = new Abstractions.ReceivedMessageContext
            {
                Body = ea.Body.ToArray(),
                RoutingKey = ea.RoutingKey ?? string.Empty,
                ExchangeName = ea.Exchange ?? string.Empty,
                QueueName = queueName,
                DeliveryTag = ea.DeliveryTag,
                MessageId = ea.BasicProperties.MessageId,
                ContentType = ea.BasicProperties.ContentType,
                Headers = ea.BasicProperties.Headers,
                Redelivered = ea.Redelivered,
                CorrelationId = ea.BasicProperties.CorrelationId,
                ReplyTo = ea.BasicProperties.ReplyTo,
                Priority = ea.BasicProperties.Priority,
                Timestamp = ea.BasicProperties.Timestamp,
                Expiration = ea.BasicProperties.Expiration,
                Channel = channel
            };

            bool shouldProcessBatch;
            List<Abstractions.ReceivedMessageContext>? batchToProcess = null;

            lock (batchLock)
            {
                batch.Add(context);
                shouldProcessBatch = batch.Count >= batchSize;
                if (shouldProcessBatch)
                {
                    batchToProcess = new List<Abstractions.ReceivedMessageContext>(batch);
                    batch.Clear();
                }
            }

            if (shouldProcessBatch && batchToProcess != null)
            {
                await ProcessBatchAsync(channel, batchToProcess, handler, cancellationToken);
            }
        };

        _ = Task.Run(async () =>
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                await Task.Delay(100, linkedCts.Token).ConfigureAwait(false);

                List<Abstractions.ReceivedMessageContext>? batchToProcess = null;

                lock (batchLock)
                {
                    if (batch.Count > 0 && (DateTime.UtcNow - lastBatchTime).TotalMilliseconds >= batchTimeoutMs)
                    {
                        batchToProcess = new List<Abstractions.ReceivedMessageContext>(batch);
                        batch.Clear();
                        lastBatchTime = DateTime.UtcNow;
                    }
                }

                if (batchToProcess != null)
                {
                    await ProcessBatchAsync(channel, batchToProcess, handler, CancellationToken.None);
                }
            }
        }, linkedCts.Token);

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumerTag: consumerTag,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer);

        _consumerTags.TryAdd(consumerTag, (channel, queueName));
        _logger.LogInformation("Started batch consuming: {Queue}, batchSize: {BatchSize}, batchTimeoutMs: {BatchTimeoutMs}",
            queueName, batchSize, batchTimeoutMs);

        return consumerTag;
    }

    private async Task ProcessBatchAsync(
        global::RabbitMQ.Client.IChannel channel,
        IList<Abstractions.ReceivedMessageContext> batchToProcess,
        Func<IList<Abstractions.ReceivedMessageContext>, Task<bool>> handler,
        CancellationToken cancellationToken)
    {
        try
        {
            var success = await handler(batchToProcess).ConfigureAwait(false);
            foreach (var msg in batchToProcess)
            {
                if (success)
                    await channel.BasicAckAsync(msg.DeliveryTag, false, cancellationToken).ConfigureAwait(false);
                else
                    await channel.BasicNackAsync(msg.DeliveryTag, false, false, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling batch");
            foreach (var msg in batchToProcess)
                await channel.BasicNackAsync(msg.DeliveryTag, false, false, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 批量消费消息 (IAsyncEnumerable 方式)
    /// </summary>
    public async IAsyncEnumerable<IList<Abstractions.ReceivedMessageContext>> ConsumeBatchAsync(
        string queueName,
        int batchSize = 10,
        int batchTimeoutMs = 1000,
        Abstractions.ConsumerOptions? options = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = await GetChannelAsync().ConfigureAwait(false);
        var batch = new List<Abstractions.ReceivedMessageContext>();
        var batchLock = new object();
        var lastBatchTime = DateTime.UtcNow;
        var batchCts = new TaskCompletionSource<IList<Abstractions.ReceivedMessageContext>>();

        await channel.BasicQosAsync(0, options?.PrefetchCount ?? 10, false).ConfigureAwait(false);

        var consumer = new global::RabbitMQ.Client.Events.AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (sender, ea) =>
        {
            var context = new Abstractions.ReceivedMessageContext
            {
                Body = ea.Body.ToArray(),
                RoutingKey = ea.RoutingKey ?? string.Empty,
                ExchangeName = ea.Exchange ?? string.Empty,
                QueueName = queueName,
                DeliveryTag = ea.DeliveryTag,
                MessageId = ea.BasicProperties.MessageId,
                ContentType = ea.BasicProperties.ContentType,
                Headers = ea.BasicProperties.Headers,
                Redelivered = ea.Redelivered,
                CorrelationId = ea.BasicProperties.CorrelationId,
                ReplyTo = ea.BasicProperties.ReplyTo,
                Priority = ea.BasicProperties.Priority,
                Timestamp = ea.BasicProperties.Timestamp,
                Expiration = ea.BasicProperties.Expiration,
                Channel = channel
            };

            lock (batchLock)
            {
                batch.Add(context);
                if (batch.Count >= batchSize)
                {
                    var batchToEmit = new List<Abstractions.ReceivedMessageContext>(batch);
                    batch.Clear();
                    lastBatchTime = DateTime.UtcNow;
                    batchCts.TrySetResult(batchToEmit);
                }
            }

            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumerTag: string.Empty,
            noLocal: false,
            exclusive: false,
            arguments: null,
            consumer: consumer).ConfigureAwait(false);

        var timerCts = new CancellationTokenSource();
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timerCts.Token);

        _ = Task.Run(async () =>
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                await Task.Delay(100, linkedCts.Token).ConfigureAwait(false);

                lock (batchLock)
                {
                    if (batch.Count > 0 && (DateTime.UtcNow - lastBatchTime).TotalMilliseconds >= batchTimeoutMs)
                    {
                        var batchToEmit = new List<Abstractions.ReceivedMessageContext>(batch);
                        batch.Clear();
                        lastBatchTime = DateTime.UtcNow;
                        batchCts.TrySetResult(batchToEmit);
                    }
                }
            }
        }, linkedCts.Token);

#if NETSTANDARD2_0
        using var registration = cancellationToken.Register(() =>
#else
        await using var registration = cancellationToken.Register(() =>
#endif
        {
            timerCts.Cancel();
            batchCts.TrySetCanceled();
        });

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                IList<Abstractions.ReceivedMessageContext>? result = null;

                try
                {
                    result = await batchCts.Task.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (result != null)
                {
                    yield return result;
                }

                batchCts = new TaskCompletionSource<IList<Abstractions.ReceivedMessageContext>>();
            }
        }
        finally
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 确认单条消息
    /// </summary>
    public ValueTask AckAsync(Abstractions.ReceivedMessageContext context)
    {
        return context.Channel.BasicAckAsync(context.DeliveryTag, false);
    }

    /// <summary>
    /// 拒绝单条消息
    /// </summary>
    public ValueTask NackAsync(Abstractions.ReceivedMessageContext context, bool requeue = false)
    {
        return context.Channel.BasicNackAsync(context.DeliveryTag, false, requeue);
    }

    /// <summary>
    /// 确认当前所有待确认消息
    /// </summary>
    public async Task AckAllAsync()
    {
        var channel = await GetChannelAsync().ConfigureAwait(false);
        await channel.BasicAckAsync(0, true).ConfigureAwait(false);
    }

    /// <summary>
    /// 拒绝当前所有待确认消息
    /// </summary>
    public async Task NackAllAsync(bool requeue = false)
    {
        var channel = await GetChannelAsync().ConfigureAwait(false);
        await channel.BasicNackAsync(0, true, requeue).ConfigureAwait(false);
    }

    /// <summary>
    /// 获取一个可用的 Channel。
    /// </summary>
    private async Task<global::RabbitMQ.Client.IChannel> GetChannelAsync()
    {
        var connection = _connectionProvider.GetConnection();
        return await connection.CreateChannelAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// 释放此消费服务占用的所有资源，包括所有活跃的 Channel。
    /// </summary>
    public void Dispose()
    {
        foreach (var (channel, _) in _consumerTags.Values)
        {
            try
            {
                channel.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing channel");
            }
        }
        _consumerTags.Clear();
        GC.SuppressFinalize(this);
    }
}
