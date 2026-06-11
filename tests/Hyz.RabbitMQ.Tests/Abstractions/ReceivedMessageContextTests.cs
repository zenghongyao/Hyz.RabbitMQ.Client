using Hyz.RabbitMQ.Abstractions;
using Moq;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class ReceivedMessageContextTests
{
    private readonly Mock<global::RabbitMQ.Client.IChannel> _mockChannel;

    public ReceivedMessageContextTests()
    {
        _mockChannel = new Mock<global::RabbitMQ.Client.IChannel>();
    }

    private ReceivedMessageContext MakeContext(
        string? messageId = "msg-123",
        string body = "test body",
        string routingKey = "test.routing.key",
        string exchangeName = "test-exchange",
        string queueName = "test-queue",
        ulong deliveryTag = 1)
    {
        return new ReceivedMessageContext
        {
            MessageId = messageId,
            Body = System.Text.Encoding.UTF8.GetBytes(body).AsMemory(),
            RoutingKey = routingKey,
            ExchangeName = exchangeName,
            QueueName = queueName,
            DeliveryTag = deliveryTag,
            Channel = _mockChannel.Object
        };
    }

    [Fact]
    public void Properties_Should_ReturnCorrectValues()
    {
        var body = System.Text.Encoding.UTF8.GetBytes("hello world");
        var headers = new Dictionary<string, object?> { { "x-header", "value" } };
        var context = new ReceivedMessageContext
        {
            MessageId = "msg-abc",
            Body = body.AsMemory(),
            RoutingKey = "rk",
            ExchangeName = "ex",
            QueueName = "q",
            Headers = headers,
            ContentType = "application/json",
            Redelivered = true,
            DeliveryTag = 42UL,
            CorrelationId = "corr-1",
            ReplyTo = "reply-queue",
            Priority = (byte?)5,
            Channel = _mockChannel.Object
        };

        Assert.Equal("msg-abc", context.MessageId);
        Assert.Equal("hello world", System.Text.Encoding.UTF8.GetString(context.Body.Span));
        Assert.Equal("rk", context.RoutingKey);
        Assert.Equal("ex", context.ExchangeName);
        Assert.Equal("q", context.QueueName);
        Assert.NotNull(context.Headers);
        Assert.Single(context.Headers);
        Assert.Equal("application/json", context.ContentType);
        Assert.True(context.Redelivered);
        Assert.Equal(42UL, context.DeliveryTag);
        Assert.Equal("corr-1", context.CorrelationId);
        Assert.Equal("reply-queue", context.ReplyTo);
        Assert.Equal((byte?)5, context.Priority);
        Assert.Equal(_mockChannel.Object, context.Channel);
    }

    [Fact]
    public async Task AckAsync_Should_CallBasicAckAsync()
    {
        var context = MakeContext(deliveryTag: 5);
        _mockChannel.Setup(c => c.BasicAckAsync(5UL, false, It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);

        await context.AckAsync();

        _mockChannel.Verify(c => c.BasicAckAsync(5UL, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task AckAsync_Should_PassCancellationToken()
    {
        var context = MakeContext(deliveryTag: 3);
        using var cts = new CancellationTokenSource();
        _mockChannel.Setup(c => c.BasicAckAsync(3UL, false, cts.Token)).Returns(ValueTask.CompletedTask);

        await context.AckAsync(cts.Token);

        _mockChannel.Verify(c => c.BasicAckAsync(3UL, false, cts.Token), Times.Once);
    }

    [Fact]
    public async Task NackAsync_WithRequeueTrue_Should_CallBasicNackAsync()
    {
        var context = MakeContext(deliveryTag: 7);
        _mockChannel.Setup(c => c.BasicNackAsync(7UL, false, true, It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);

        await context.NackAsync(requeue: true);

        _mockChannel.Verify(c => c.BasicNackAsync(7UL, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task NackAsync_WithRequeueFalse_Should_CallBasicNackAsync()
    {
        var context = MakeContext(deliveryTag: 9);
        _mockChannel.Setup(c => c.BasicNackAsync(9UL, false, false, It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);

        await context.NackAsync(requeue: false);

        _mockChannel.Verify(c => c.BasicNackAsync(9UL, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task NackAsync_Should_PassCancellationToken()
    {
        var context = MakeContext(deliveryTag: 11);
        using var cts = new CancellationTokenSource();
        _mockChannel.Setup(c => c.BasicNackAsync(11UL, false, true, cts.Token)).Returns(ValueTask.CompletedTask);

        await context.NackAsync(requeue: true, cts.Token);

        _mockChannel.Verify(c => c.BasicNackAsync(11UL, false, true, cts.Token), Times.Once);
    }

    [Fact]
    public void Body_Should_BeReadableAsString()
    {
        var context = MakeContext(body: "Hello RabbitMQ!");
        Assert.Equal("Hello RabbitMQ!", System.Text.Encoding.UTF8.GetString(context.Body.Span));
    }

    [Fact]
    public void WithNullOptionalFields_Should_AllowNullValues()
    {
        var context = new ReceivedMessageContext
        {
            Body = Array.Empty<byte>().AsMemory(),
            RoutingKey = "key",
            ExchangeName = "ex",
            QueueName = "q",
            DeliveryTag = 1,
            Channel = _mockChannel.Object,
            MessageId = null,
            Headers = null,
            ContentType = null,
            CorrelationId = null,
            ReplyTo = null,
            Priority = null,
            Expiration = null,
            Timestamp = null
        };

        Assert.Null(context.MessageId);
        Assert.Null(context.Headers);
        Assert.Null(context.ContentType);
        Assert.Null(context.CorrelationId);
        Assert.Null(context.ReplyTo);
        Assert.Null(context.Priority);
        Assert.Null(context.Expiration);
        Assert.Null(context.Timestamp);
    }
}
