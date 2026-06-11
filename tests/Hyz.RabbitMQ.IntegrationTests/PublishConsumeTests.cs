using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Abstractions;
using global::RabbitMQ.Client;

namespace Hyz.RabbitMQ.IntegrationTests;

/// <summary>
/// T-INT-001: EndToEnd_PublishAndConsume - 端到端发布消费测试
/// </summary>
[RequiresDocker]
public class EndToEnd_PublishAndConsume_Tests : IntegrationTestBase
{
    [Fact]
    public async Task PublishToQueue_Should_ConsumeTheSameMessage()
    {
        EnsureConnected();

        var queueName = $"test-publish-{Guid.NewGuid():N}";
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

        var message = MessageBody.FromString("Hello RabbitMQ Integration Test!");
        await publisher.PublishAsync(queueName, message);

        await Task.Delay(500);

        var consumed = await ConsumeMessageAsync(channel, queueName);
        Assert.NotNull(consumed);
        Assert.Equal("Hello RabbitMQ Integration Test!", consumed.Value.Body);

        await channel.BasicAckAsync(consumed.Value.DeliveryTag, false);

        provider.Dispose();
    }

    [Fact]
    public async Task PublishToExchange_Should_ConsumeViaRoutingKey()
    {
        EnsureConnected();

        var exchangeName = $"test-exchange-{Guid.NewGuid():N}";
        var queueName = $"test-queue-{Guid.NewGuid():N}";
        var routingKey = "test.routing.key";

        var channel = await CreateChannelAsync();
        await DeclareExchangeAsync(channel, exchangeName);
        await DeclareQueueAsync(channel, queueName);
        await BindQueueAsync(channel, queueName, exchangeName, routingKey);

        var provider = new RabbitMqConnectionProvider(
            new RabbitMqConnectionOptions
            {
                HostName = RabbitMqHost,
                Port = RabbitMqPort,
                UserName = RabbitMqUser,
                Password = RabbitMqPass
            });

        var publisher = new PublisherService(provider);

        var message = MessageBody.FromString("Exchange routing test");
        await publisher.PublishToExchangeAsync(exchangeName, routingKey, message);

        await Task.Delay(500);

        var consumed = await ConsumeMessageAsync(channel, queueName);
        Assert.NotNull(consumed);
        Assert.Equal("Exchange routing test", consumed.Value.Body);

        await channel.BasicAckAsync(consumed.Value.DeliveryTag, false);

        provider.Dispose();
    }
}

/// <summary>
/// T-INT-002: EndToEnd_BatchProcessing - 批量处理集成测试
/// </summary>
[RequiresDocker]
public class EndToEnd_BatchProcessing_Tests : IntegrationTestBase
{
    [Fact]
    public async Task PublishBatch_Should_DeliverAllMessages()
    {
        EnsureConnected();

        var queueName = $"test-batch-{Guid.NewGuid():N}";
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

        var messages = Enumerable.Range(1, 5)
            .Select(i => MessageBody.FromString($"Batch message {i}"))
            .ToList();

        await publisher.PublishBatchAsync(string.Empty, queueName, messages);

        await Task.Delay(1000);

        var consumed = new List<(string Body, ulong Tag)>();
        for (int i = 0; i < 5; i++)
        {
            var result = await ConsumeMessageAsync(channel, queueName);
            if (result != null)
            {
                consumed.Add(result.Value);
            }
        }

        Assert.Equal(5, consumed.Count);
        for (int i = 0; i < 5; i++)
        {
            await channel.BasicAckAsync(consumed[i].Tag, false);
        }

        provider.Dispose();
    }

    [Fact]
    public async Task PublishBatchAsync_Should_ReturnCorrectSuccessCount()
    {
        EnsureConnected();

        var queueName = $"test-batch-count-{Guid.NewGuid():N}";
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

        var messages = Enumerable.Range(1, 3)
            .Select(i => MessageBody.FromString($"Count test {i}"))
            .ToList();

        var result = await publisher.PublishBatchAsync(string.Empty, queueName, messages);

        Assert.Equal(3, result.SuccessCount);
        Assert.Equal(0, result.FailedCount);

        provider.Dispose();
    }
}
