using global::RabbitMQ.Client;
using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.IntegrationTests;

/// <summary>
/// T-INT-003: EndToEnd_MultiConnection - 多连接隔离测试
/// </summary>
[RequiresDocker]
public class EndToEnd_MultiConnection_Tests : IntegrationTestBase_NoSharedConnection
{
    [Fact]
    public void MultipleConnections_Should_BeIndependent()
    {
        EnsureConnected();

        var options1 = new RabbitMqConnectionOptions
        {
            Name = "conn-1",
            HostName = RabbitMqHost,
            Port = RabbitMqPort,
            UserName = RabbitMqUser,
            Password = RabbitMqPass
        };
        var options2 = new RabbitMqConnectionOptions
        {
            Name = "conn-2",
            HostName = RabbitMqHost,
            Port = RabbitMqPort,
            UserName = RabbitMqUser,
            Password = RabbitMqPass
        };

        var provider1 = new RabbitMqConnectionProvider(options1);
        var provider2 = new RabbitMqConnectionProvider(options2);

        var manager = new ConnectionManager();
        manager.Register(provider1, isDefault: true);
        manager.Register(provider2);

        Assert.Equal("conn-1", manager.Default.Name);
        Assert.True(manager.Contains("conn-1"));
        Assert.True(manager.Contains("conn-2"));
        Assert.Equal(2, manager.GetAllConnectionNames().Count);

        provider1.Dispose();
        provider2.Dispose();
    }
}

/// <summary>
/// T-INT-004: EndToEnd_Reconnection - 断线重连测试
/// </summary>
[RequiresDocker]
public class EndToEnd_Reconnection_Tests : IntegrationTestBase_NoSharedConnection
{
    [Fact]
    public async Task Connection_Should_ReconnectOnFailure()
    {
        EnsureConnected();

        var options = new RabbitMqConnectionOptions
        {
            HostName = RabbitMqHost,
            Port = RabbitMqPort,
            UserName = RabbitMqUser,
            Password = RabbitMqPass,
            AutoReconnect = true,
            RetryDelayMs = 1000
        };

        var provider = new RabbitMqConnectionProvider(options);
        await provider.GetConnectionAsync();

        await provider.TryReconnectAsync();

        var state = provider.State;
        Assert.True(state == ConnectionState.Open || state == ConnectionState.Opening);

        provider.Dispose();
    }

    [Fact]
    public void ConnectionManager_Should_ThrowWhenConnectionNotFound()
    {
        var manager = new ConnectionManager();

        var ex = Assert.Throws<KeyNotFoundException>(() => manager.GetProvider("non-existent"));
        Assert.Contains("not found", ex.Message);
    }
}

/// <summary>
/// T-INT-005: EndToEnd_PublisherConfirms - 发布确认测试
/// </summary>
[RequiresDocker]
public class EndToEnd_PublisherConfirms_Tests : IntegrationTestBase
{
    [Fact]
    public async Task PublishWithConfirmation_Should_ReturnSuccess_WhenConnected()
    {
        EnsureConnected();

        var queueName = $"test-confirm-{Guid.NewGuid():N}";
        var channel = await CreateChannelAsync();
        await DeclareQueueAsync(channel, queueName);

        var provider = new RabbitMqConnectionProvider(
            new RabbitMqConnectionOptions
            {
                HostName = RabbitMqHost,
                Port = RabbitMqPort,
                UserName = RabbitMqUser,
                Password = RabbitMqPass
            });

        var publisher = new PublisherService(provider);

        var message = MessageBody.FromString("Confirmation test");
        var result = await publisher.PublishWithConfirmationAsync(string.Empty, queueName, message);

        Assert.NotNull(result);

        provider.Dispose();
    }

    [Fact]
    public async Task PublishResult_Should_IndicateSuccess()
    {
        EnsureConnected();

        var queueName = $"test-result-{Guid.NewGuid():N}";
        var channel = await CreateChannelAsync();
        await DeclareQueueAsync(channel, queueName);

        var provider = new RabbitMqConnectionProvider(
            new RabbitMqConnectionOptions
            {
                HostName = RabbitMqHost,
                Port = RabbitMqPort,
                UserName = RabbitMqUser,
                Password = RabbitMqPass
            });

        var publisher = new PublisherService(provider);

        var message = MessageBody.FromString("Result test");
        var result = await publisher.PublishWithConfirmationAsync(string.Empty, queueName, message);

        Assert.True(result.IsSuccess);
        Assert.Null(result.ErrorMessage);

        provider.Dispose();
    }
}

/// <summary>
/// T-INT-006: EndToEnd_DeadLetter - 死信队列测试
/// </summary>
[RequiresDocker]
public class EndToEnd_DeadLetter_Tests : IntegrationTestBase
{
    [Fact]
    public async Task Queue_WithDeadLetterExchange_Should_RouteRejectedMessages()
    {
        EnsureConnected();

        var mainQueue = $"test-dlx-main-{Guid.NewGuid():N}";
        var deadLetterQueue = $"test-dlx-dlq-{Guid.NewGuid():N}";
        var dlxExchange = $"test-dlx-exchange-{Guid.NewGuid():N}";

        var channel = await CreateChannelAsync();

        await DeclareExchangeAsync(channel, dlxExchange, "direct");
        await DeclareQueueAsync(channel, deadLetterQueue);
        await BindQueueAsync(channel, deadLetterQueue, dlxExchange, "dead-letter-routing-key");

        var args = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", dlxExchange },
            { "x-dead-letter-routing-key", "dead-letter-routing-key" }
        };
        await channel.QueueDeclareAsync(
            queue: mainQueue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: args);
        _createdQueues.Add(mainQueue);

        await PublishMessageAsync(channel, string.Empty, mainQueue, "Message to be dead-lettered");

        await Task.Delay(300);

        var result = await ConsumeMessageAsync(channel, mainQueue);
        Assert.NotNull(result);
        await channel.BasicNackAsync(result.Value.DeliveryTag, false, requeue: false);

        await Task.Delay(300);

        var dlqResult = await ConsumeMessageAsync(channel, deadLetterQueue);
        Assert.NotNull(dlqResult);
        Assert.Equal("Message to be dead-lettered", dlqResult.Value.Body);

        await channel.BasicAckAsync(dlqResult.Value.DeliveryTag, false);
    }

    [Fact]
    public async Task Queue_WithMaxLength_Should_RejectOverflow()
    {
        EnsureConnected();

        var queueName = $"test-maxlen-{Guid.NewGuid():N}";
        var channel = await CreateChannelAsync();

        var args = new Dictionary<string, object?>
        {
            { "x-max-length", 2 }
        };
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: args);
        _createdQueues.Add(queueName);

        await channel.BasicPublishAsync("", queueName, false, new BasicProperties { Persistent = true },
            System.Text.Encoding.UTF8.GetBytes("msg1"));
        await channel.BasicPublishAsync("", queueName, false, new BasicProperties { Persistent = true },
            System.Text.Encoding.UTF8.GetBytes("msg2"));
        await channel.BasicPublishAsync("", queueName, false, new BasicProperties { Persistent = true },
            System.Text.Encoding.UTF8.GetBytes("msg3"));

        await Task.Delay(300);

        var queueInfo = await channel.QueueDeclarePassiveAsync(queueName);
        Assert.Equal(2u, queueInfo.MessageCount);
    }
}
