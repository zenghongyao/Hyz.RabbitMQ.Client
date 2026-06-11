using global::RabbitMQ.Client;

namespace Hyz.RabbitMQ.IntegrationTests;

/// <summary>
/// 集成测试基类：管理 RabbitMQ 连接、通道和资源清理
/// 当 Docker 不可用时，连接为 null，测试应先调用 EnsureConnected()
/// </summary>
public abstract class IntegrationTestBase : IDisposable
{
    private readonly IConnection? _connection;
    private readonly List<IChannel> _channels = new();
    protected readonly List<string> _createdQueues = new();
    private readonly List<string> _createdExchanges = new();

    protected static string RabbitMqHost => Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
    protected static int RabbitMqPort => int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var p) ? p : 5672;
    protected static string RabbitMqUser => Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest";
    protected static string RabbitMqPass => Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest";

    protected IConnection? Connection => _connection;

    protected IntegrationTestBase()
    {
        if (!IsRabbitMqAvailable())
            return;

        var factory = new ConnectionFactory
        {
            HostName = RabbitMqHost,
            Port = RabbitMqPort,
            UserName = RabbitMqUser,
            Password = RabbitMqPass
        };

        try
        {
            _connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        }
        catch
        {
            // Connection failed, tests will skip via EnsureConnected
        }
    }

    /// <summary>
    /// 确保 RabbitMQ 可用，否则抛出 SkipException
    /// </summary>
    protected void EnsureConnected()
    {
        if (!IsRabbitMqAvailable())
        {
            throw new Exception(
                "SKIP: RabbitMQ Docker not available. " +
                "Start: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management " +
                "Or set SKIP_DOCKER_TESTS=1");
        }
        if (_connection == null)
        {
            throw new Exception(
                "SKIP: Could not connect to RabbitMQ. " +
                "Start: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management");
        }
    }

    public static bool IsRabbitMqAvailable()
    {
        if (Environment.GetEnvironmentVariable("SKIP_DOCKER_TESTS") == "1")
            return false;

        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "ps --filter name=rabbitmq --format {{.Names}}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var process = System.Diagnostics.Process.Start(psi);
            if (process == null) return false;
            var output = process.StandardOutput.ReadToEnd().Trim();
            process.WaitForExit(5000);
            return process.ExitCode == 0 && output.Contains("rabbitmq");
        }
        catch
        {
            return false;
        }
    }

    protected IChannel CreateChannel()
    {
        if (_connection == null) throw new Exception("SKIP: No connection available");
        var channel = _connection.CreateChannelAsync(null, CancellationToken.None).GetAwaiter().GetResult();
        _channels.Add(channel);
        return channel;
    }

    protected async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        if (_connection == null) throw new Exception("SKIP: No connection available");
        var channel = await _connection.CreateChannelAsync(null, cancellationToken);
        _channels.Add(channel);
        return channel;
    }

    protected async Task<string> DeclareQueueAsync(IChannel channel, string queueName, CancellationToken cancellationToken = default)
    {
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);
        _createdQueues.Add(queueName);
        return queueName;
    }

    protected async Task DeclareExchangeAsync(IChannel channel, string exchangeName, string type = "direct", CancellationToken cancellationToken = default)
    {
        await channel.ExchangeDeclareAsync(
            exchange: exchangeName,
            type: type,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken);
        _createdExchanges.Add(exchangeName);
    }

    protected async Task BindQueueAsync(IChannel channel, string queueName, string exchangeName, string routingKey, CancellationToken cancellationToken = default)
    {
        await channel.QueueBindAsync(
            queue: queueName,
            exchange: exchangeName,
            routingKey: routingKey,
            arguments: null,
            cancellationToken: cancellationToken);
    }

    protected async Task PublishMessageAsync(IChannel channel, string exchangeName, string routingKey, string body, CancellationToken cancellationToken = default)
    {
        var props = new BasicProperties { Persistent = true };
        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: System.Text.Encoding.UTF8.GetBytes(body),
            cancellationToken: cancellationToken);
    }

    protected async Task<(string Body, ulong DeliveryTag)?> ConsumeMessageAsync(IChannel channel, string queueName, CancellationToken cancellationToken = default)
    {
        var result = await channel.BasicGetAsync(queue: queueName, autoAck: false, cancellationToken: cancellationToken);
        if (result == null) return null;
        return (System.Text.Encoding.UTF8.GetString(result.Body.ToArray()), result.DeliveryTag);
    }

    public void Dispose()
    {
        foreach (var queue in _createdQueues)
        {
            try
            {
                if (_connection == null) continue;
                var ch = _connection.CreateChannelAsync(null, CancellationToken.None).GetAwaiter().GetResult();
                ch.QueueDeleteAsync(queue, ifUnused: false, ifEmpty: false).GetAwaiter().GetResult();
                ch.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch { }
        }
        foreach (var exch in _createdExchanges)
        {
            try
            {
                if (_connection == null) continue;
                var ch = _connection.CreateChannelAsync(null, CancellationToken.None).GetAwaiter().GetResult();
                ch.ExchangeDeleteAsync(exchange: exch, ifUnused: false).GetAwaiter().GetResult();
                ch.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch { }
        }
        foreach (var ch in _channels)
        {
            try { ch.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        }
        if (_connection != null)
        {
            try { _connection.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
        }
        GC.SuppressFinalize(this);
    }
}
