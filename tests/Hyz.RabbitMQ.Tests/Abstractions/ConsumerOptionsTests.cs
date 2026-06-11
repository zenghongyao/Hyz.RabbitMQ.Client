using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class ConsumerOptionsTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Act
        var options = new ConsumerOptions();

        // Assert
        Assert.False(options.AutoAck);
        Assert.False(options.Exclusive);
        Assert.False(options.AutoDelete);
        Assert.Equal(10, options.PrefetchCount);
        Assert.Equal(0, options.Priority);
        Assert.Null(options.ConsumerTag);
        Assert.Null(options.Arguments);
    }

    [Fact]
    public void WithConsumerTag_Should_SetConsumerTag()
    {
        // Arrange
        string consumerTag = "consumer-1";

        // Act
        var options = new ConsumerOptions { ConsumerTag = consumerTag };

        // Assert
        Assert.Equal(consumerTag, options.ConsumerTag);
    }

    [Fact]
    public void WithAutoAck_Should_SetAutoAck()
    {
        // Act
        var options = new ConsumerOptions { AutoAck = true };

        // Assert
        Assert.True(options.AutoAck);
    }

    [Fact]
    public void WithPrefetchCount_Should_SetPrefetchCount()
    {
        // Act
        var options = new ConsumerOptions { PrefetchCount = 50 };

        // Assert
        Assert.Equal(50, options.PrefetchCount);
    }

    [Fact]
    public void WithExclusive_Should_SetExclusive()
    {
        // Act
        var options = new ConsumerOptions { Exclusive = true };

        // Assert
        Assert.True(options.Exclusive);
    }

    [Fact]
    public void WithPriority_Should_SetPriority()
    {
        // Act
        var options = new ConsumerOptions { Priority = 5 };

        // Assert
        Assert.Equal(5, options.Priority);
    }

    [Fact]
    public void WithAutoDelete_Should_SetAutoDelete()
    {
        // Act
        var options = new ConsumerOptions { AutoDelete = true };

        // Assert
        Assert.True(options.AutoDelete);
    }

    [Fact]
    public void WithArguments_Should_SetArguments()
    {
        // Arrange
        var arguments = new Dictionary<string, object?>
        {
            { "x-max-length", 1000 }
        };

        // Act
        var options = new ConsumerOptions { Arguments = arguments };

        // Assert
        Assert.NotNull(options.Arguments);
        Assert.Single(options.Arguments);
    }

    [Fact]
    public void Default_PrefetchCount_Should_Be10()
    {
        // Act
        var options = new ConsumerOptions();

        // Assert
        Assert.Equal((ushort)10, options.PrefetchCount);
    }

    [Fact]
    public void Default_Priority_Should_BeZero()
    {
        // Act
        var options = new ConsumerOptions();

        // Assert
        Assert.Equal((byte)0, options.Priority);
    }
}
