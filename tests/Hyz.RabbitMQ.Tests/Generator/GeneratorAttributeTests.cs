using Hyz.RabbitMQ.Abstractions.Attributes;

namespace Hyz.RabbitMQ.Tests.Generator;

public class RabbitMqConsumerAttributeTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var attribute = new RabbitMqConsumerAttribute { Queue = "test-queue" };

        // Assert
        Assert.Equal("test-queue", attribute.Queue);
        Assert.Null(attribute.Exchange);
        Assert.Null(attribute.RoutingKey);
        Assert.Null(attribute.ConnectionName);
        Assert.False(attribute.AutoAck);
        Assert.Equal((ushort)10, attribute.PrefetchCount);
        Assert.True(attribute.Durable);
        Assert.Equal(3, attribute.MaxRetryCount);
    }

    [Fact]
    public void WithAllProperties_Should_SetAllProperties()
    {
        // Arrange & Act
        var attribute = new RabbitMqConsumerAttribute
        {
            Queue = "my-queue",
            Exchange = "my-exchange",
            RoutingKey = "my-routing-key",
            ConnectionName = "my-connection",
            TagPrefix = "prefix",
            AutoAck = true,
            PrefetchCount = 50,
            Durable = false,
            MaxRetryCount = 5,
            DeadLetterExchange = "dlx",
            DeadLetterRoutingKey = "dlrk"
        };

        // Assert
        Assert.Equal("my-queue", attribute.Queue);
        Assert.Equal("my-exchange", attribute.Exchange);
        Assert.Equal("my-routing-key", attribute.RoutingKey);
        Assert.Equal("my-connection", attribute.ConnectionName);
        Assert.Equal("prefix", attribute.TagPrefix);
        Assert.True(attribute.AutoAck);
        Assert.Equal((ushort)50, attribute.PrefetchCount);
        Assert.False(attribute.Durable);
        Assert.Equal(5, attribute.MaxRetryCount);
        Assert.Equal("dlx", attribute.DeadLetterExchange);
        Assert.Equal("dlrk", attribute.DeadLetterRoutingKey);
    }
}

public class RabbitMqSubscribeAttributeTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var attribute = new RabbitMqSubscribeAttribute { Queue = "test-queue" };

        // Assert
        Assert.Equal("test-queue", attribute.Queue);
        Assert.Null(attribute.Exchange);
        Assert.False(attribute.AutoAck);
        Assert.Equal((ushort)10, attribute.PrefetchCount);
        Assert.True(attribute.Durable);
        Assert.Equal(3, attribute.MaxRetryCount);
        Assert.True(attribute.UseDedicatedThread);
        Assert.Equal(0, attribute.StartupPriority);
    }

    [Fact]
    public void WithAllProperties_Should_SetAllProperties()
    {
        // Arrange & Act
        var attribute = new RabbitMqSubscribeAttribute
        {
            Queue = "subscribe-queue",
            Exchange = "subscribe-exchange",
            RoutingKey = "subscribe-key",
            ConnectionName = "conn-1",
            AutoAck = true,
            PrefetchCount = 100,
            Durable = false,
            MaxRetryCount = 10,
            DeadLetterExchange = "dlx",
            DeadLetterRoutingKey = "dlrk",
            UseDedicatedThread = false,
            ThreadName = "CustomThread",
            StartupPriority = 5
        };

        // Assert
        Assert.Equal("subscribe-queue", attribute.Queue);
        Assert.Equal("subscribe-exchange", attribute.Exchange);
        Assert.Equal("subscribe-key", attribute.RoutingKey);
        Assert.Equal("conn-1", attribute.ConnectionName);
        Assert.True(attribute.AutoAck);
        Assert.Equal((ushort)100, attribute.PrefetchCount);
        Assert.False(attribute.Durable);
        Assert.Equal(10, attribute.MaxRetryCount);
        Assert.Equal("dlx", attribute.DeadLetterExchange);
        Assert.Equal("dlrk", attribute.DeadLetterRoutingKey);
        Assert.False(attribute.UseDedicatedThread);
        Assert.Equal("CustomThread", attribute.ThreadName);
        Assert.Equal(5, attribute.StartupPriority);
    }
}

public class RabbitMqBatchSubscribeAttributeTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var attribute = new RabbitMqBatchSubscribeAttribute { Queue = "batch-queue" };

        // Assert
        Assert.Equal("batch-queue", attribute.Queue);
        Assert.Equal(10, attribute.BatchSize);
        Assert.Equal(1000, attribute.BatchTimeoutMs);
        Assert.Null(attribute.Exchange);
        Assert.Null(attribute.RoutingKey);
        Assert.Null(attribute.ConnectionName);
        Assert.Equal((ushort)50, attribute.PrefetchCount);
    }

    [Fact]
    public void WithAllProperties_Should_SetAllProperties()
    {
        // Arrange & Act
        var attribute = new RabbitMqBatchSubscribeAttribute
        {
            Queue = "batch-queue",
            BatchSize = 50,
            BatchTimeoutMs = 5000,
            Exchange = "batch-exchange",
            RoutingKey = "batch-key",
            ConnectionName = "batch-connection",
            PrefetchCount = 200
        };

        // Assert
        Assert.Equal("batch-queue", attribute.Queue);
        Assert.Equal(50, attribute.BatchSize);
        Assert.Equal(5000, attribute.BatchTimeoutMs);
        Assert.Equal("batch-exchange", attribute.Exchange);
        Assert.Equal("batch-key", attribute.RoutingKey);
        Assert.Equal("batch-connection", attribute.ConnectionName);
        Assert.Equal((ushort)200, attribute.PrefetchCount);
    }
}

public class RabbitMqExchangeAttributeTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var attribute = new RabbitMqExchangeAttribute { Name = "test-exchange" };

        // Assert
        Assert.Equal("test-exchange", attribute.Name);
        Assert.Equal("Direct", attribute.Type);
        Assert.True(attribute.Durable);
        Assert.False(attribute.AutoDelete);
    }

    [Fact]
    public void WithAllProperties_Should_SetAllProperties()
    {
        // Arrange & Act
        var attribute = new RabbitMqExchangeAttribute
        {
            Name = "my-exchange",
            Type = "Topic",
            Durable = false,
            AutoDelete = true,
            Arguments = "{\"alternate-exchange\":\"alt-ex\"}"
        };

        // Assert
        Assert.Equal("my-exchange", attribute.Name);
        Assert.Equal("Topic", attribute.Type);
        Assert.False(attribute.Durable);
        Assert.True(attribute.AutoDelete);
        Assert.Equal("{\"alternate-exchange\":\"alt-ex\"}", attribute.Arguments);
    }
}

public class RabbitMqQueueAttributeTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var attribute = new RabbitMqQueueAttribute { Name = "test-queue" };

        // Assert
        Assert.Equal("test-queue", attribute.Name);
        Assert.True(attribute.Durable);
        Assert.False(attribute.Exclusive);
        Assert.False(attribute.AutoDelete);
        Assert.Null(attribute.MessageTtl);
        Assert.Null(attribute.MaxLength);
    }

    [Fact]
    public void WithAllProperties_Should_SetAllProperties()
    {
        // Arrange & Act
        var attribute = new RabbitMqQueueAttribute
        {
            Name = "my-queue",
            Durable = false,
            Exclusive = true,
            AutoDelete = true,
            MessageTtl = 60000,
            MaxLength = 1000,
            DeadLetterExchange = "dlx",
            DeadLetterRoutingKey = "dlrk",
            Arguments = "{\"x-max-priority\":10}"
        };

        // Assert
        Assert.Equal("my-queue", attribute.Name);
        Assert.False(attribute.Durable);
        Assert.True(attribute.Exclusive);
        Assert.True(attribute.AutoDelete);
        Assert.Equal(60000, attribute.MessageTtl);
        Assert.Equal(1000, attribute.MaxLength);
        Assert.Equal("dlx", attribute.DeadLetterExchange);
        Assert.Equal("dlrk", attribute.DeadLetterRoutingKey);
        Assert.Equal("{\"x-max-priority\":10}", attribute.Arguments);
    }
}

public class RabbitMqBindingAttributeTests
{
    [Fact]
    public void Default_Should_SetExchangeAndRoutingKey()
    {
        // Arrange & Act
        var attribute = new RabbitMqBindingAttribute
        {
            Exchange = "my-exchange",
            RoutingKey = "my-routing-key"
        };

        // Assert
        Assert.Equal("my-exchange", attribute.Exchange);
        Assert.Equal("my-routing-key", attribute.RoutingKey);
    }
}
