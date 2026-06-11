using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Sample.Rpc;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .ConfigureServices((context, services) =>
            {
                services.AddRabbitMq(options =>
                {
                    options.HostName = "localhost";
                    options.UserName = "guest";
                    options.Password = "guest";
                });

                services.AddHostedService<RpcServerHostedService>();
                services.AddHostedService<RpcClientHostedService>();
            })
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Information);
            })
            .Build();

        await host.RunAsync();
    }
}

public class RpcRequest
{
    public int Number { get; set; }
    public string? Operation { get; set; }
}

public class RpcResponse
{
    public int Result { get; set; }
    public bool Success { get; set; }
    public string? Error { get; set; }
}

public class RpcServerHostedService : BackgroundService
{
    private readonly IConnectionManager _connectionManager;
    private readonly ILogger<RpcServerHostedService> _logger;
    private const string RpcQueueName = "rpc.queue";

    public RpcServerHostedService(
        IConnectionManager connectionManager,
        ILogger<RpcServerHostedService> logger)
    {
        _connectionManager = connectionManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("RPC Server starting...");

        var provider = _connectionManager.Default;
        var connection = await provider.GetConnectionAsync(stoppingToken);
        var channel = await connection.CreateChannelAsync(null, stoppingToken);

        await channel.QueueDeclareAsync(
            queue: RpcQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        _logger.LogInformation("RPC Server listening on queue: {Queue}", RpcQueueName);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            try
            {
                var requestBody = ea.Body.ToArray();
                var requestJson = System.Text.Encoding.UTF8.GetString(requestBody);
                _logger.LogInformation("Received RPC request: {Request}", requestJson);

                var request = System.Text.Json.JsonSerializer.Deserialize<RpcRequest>(requestJson);
                var result = Calculate(request!);

                var response = new RpcResponse { Result = result, Success = true };
                var responseJson = System.Text.Json.JsonSerializer.Serialize(response);

                var props = new BasicProperties();
                props.CorrelationId = ea.BasicProperties.CorrelationId;
                props.ReplyTo = ea.BasicProperties.ReplyTo;

                await channel.BasicPublishAsync(
                    exchange: string.Empty,
                    routingKey: ea.BasicProperties.ReplyTo ?? string.Empty,
                    mandatory: false,
                    basicProperties: props,
                    body: System.Text.Encoding.UTF8.GetBytes(responseJson),
                    cancellationToken: stoppingToken);

                await channel.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
                _logger.LogInformation("RPC response sent: {Response}", responseJson);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing RPC request");
                await channel.BasicNackAsync(ea.DeliveryTag, false, true, stoppingToken);
            }
        };

        await channel.BasicConsumeAsync(
            queue: RpcQueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private static int Calculate(RpcRequest request)
    {
        return request.Operation?.ToLower() switch
        {
            "add" => request.Number + 10,
            "multiply" => request.Number * 2,
            "square" => request.Number * request.Number,
            _ => request.Number
        };
    }
}

public class RpcClientHostedService : BackgroundService
{
    private readonly IConnectionManager _connectionManager;
    private readonly ILogger<RpcClientHostedService> _logger;

    public RpcClientHostedService(
        IConnectionManager connectionManager,
        ILogger<RpcClientHostedService> logger)
    {
        _connectionManager = connectionManager;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("RPC Client starting...");

        await Task.Delay(2000, stoppingToken);

        var provider = _connectionManager.Default;
        var connection = await provider.GetConnectionAsync(stoppingToken);

        var requests = new[]
        {
            new RpcRequest { Number = 5, Operation = "add" },
            new RpcRequest { Number = 3, Operation = "multiply" },
            new RpcRequest { Number = 4, Operation = "square" }
        };

        foreach (var request in requests)
        {
            try
            {
                var result = await CallRpcAsync(connection, request, stoppingToken);
                _logger.LogInformation("RPC Call({Operation}, {Number}) = {Result}",
                    request.Operation, request.Number, result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "RPC call failed");
            }
        }

        _logger.LogInformation("RPC Client completed all requests");
    }

    private async Task<int> CallRpcAsync(
        IConnection connection,
        RpcRequest request,
        CancellationToken cancellationToken)
    {
        var channel = await connection.CreateChannelAsync(null, cancellationToken);
        var replyQueue = await channel.QueueDeclareAsync(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null,
            cancellationToken: cancellationToken);

        var tcs = new TaskCompletionSource<int>();
        var correlationId = Guid.NewGuid().ToString();

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (sender, ea) =>
        {
            if (ea.BasicProperties.CorrelationId == correlationId)
            {
                var responseJson = System.Text.Encoding.UTF8.GetString(ea.Body.ToArray());
                var response = System.Text.Json.JsonSerializer.Deserialize<RpcResponse>(responseJson);
                tcs.TrySetResult(response!.Result);
            }
            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: replyQueue,
            autoAck: true,
            consumer: consumer,
            cancellationToken: cancellationToken);

        var props = new BasicProperties();
        props.CorrelationId = correlationId;
        props.ReplyTo = replyQueue;

        var requestJson = System.Text.Json.JsonSerializer.Serialize(request);
        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: "rpc.queue",
            mandatory: false,
            basicProperties: props,
            body: System.Text.Encoding.UTF8.GetBytes(requestJson),
            cancellationToken: cancellationToken);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            var completed = await Task.WhenAny(tcs.Task, Task.Delay(Timeout.Infinite, timeoutCts.Token));
            return completed == tcs.Task ? tcs.Task.Result : throw new TimeoutException("RPC call timed out");
        }
        finally
        {
            await channel.CloseAsync(cancellationToken);
        }
    }
}
