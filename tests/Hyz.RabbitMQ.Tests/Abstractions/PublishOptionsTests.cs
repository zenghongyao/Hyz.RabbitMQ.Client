using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class PublishOptionsTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Act
        var options = new PublishOptions();

        // Assert
        Assert.True(options.Persistent);
        Assert.False(options.Mandatory);
        Assert.Null(options.ContentType);
        Assert.Null(options.ContentEncoding);
        Assert.Null(options.Headers);
        Assert.Null(options.CorrelationId);
        Assert.Null(options.ReplyTo);
        Assert.Null(options.Expiration);
        Assert.Equal(0, options.Priority);
        Assert.Null(options.MessageId);
        Assert.Null(options.Timestamp);
        Assert.Null(options.ProducerId);
    }

    [Fact]
    public void WithMessageId_Should_SetMessageId()
    {
        // Arrange
        string messageId = "msg-123";

        // Act
        var options = new PublishOptions { MessageId = messageId };

        // Assert
        Assert.Equal(messageId, options.MessageId);
    }

    [Fact]
    public void WithCorrelationId_Should_SetCorrelationId()
    {
        // Arrange
        string correlationId = "corr-456";

        // Act
        var options = new PublishOptions { CorrelationId = correlationId };

        // Assert
        Assert.Equal(correlationId, options.CorrelationId);
    }

    [Fact]
    public void WithHeaders_Should_SetHeaders()
    {
        // Arrange
        var headers = new Dictionary<string, object?>
        {
            { "x-custom", "value" }
        };

        // Act
        var options = new PublishOptions { Headers = headers };

        // Assert
        Assert.NotNull(options.Headers);
        Assert.Single(options.Headers);
    }

    [Fact]
    public void WithMandatory_Should_SetMandatory()
    {
        // Act
        var options = new PublishOptions { Mandatory = true };

        // Assert
        Assert.True(options.Mandatory);
    }

    [Fact]
    public void WithPersistent_Should_SetPersistent()
    {
        // Act
        var options = new PublishOptions { Persistent = false };

        // Assert
        Assert.False(options.Persistent);
    }

    [Fact]
    public void WithContentType_Should_SetContentType()
    {
        // Act
        var options = new PublishOptions { ContentType = "text/plain" };

        // Assert
        Assert.Equal("text/plain", options.ContentType);
    }

    [Fact]
    public void WithReplyTo_Should_SetReplyTo()
    {
        // Arrange
        string replyTo = "reply.queue";

        // Act
        var options = new PublishOptions { ReplyTo = replyTo };

        // Assert
        Assert.Equal(replyTo, options.ReplyTo);
    }

    [Fact]
    public void WithPriority_Should_SetPriority()
    {
        // Act
        var options = new PublishOptions { Priority = 5 };

        // Assert
        Assert.Equal(5, options.Priority);
    }

    [Fact]
    public void WithExpiration_Should_SetExpiration()
    {
        // Arrange
        var expiration = TimeSpan.FromMinutes(5);

        // Act
        var options = new PublishOptions { Expiration = expiration };

        // Assert
        Assert.Equal(expiration, options.Expiration);
    }

    [Fact]
    public void WithTimestamp_Should_SetTimestamp()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;

        // Act
        var options = new PublishOptions { Timestamp = timestamp };

        // Assert
        Assert.Equal(timestamp, options.Timestamp);
    }
}
