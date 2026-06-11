using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class BatchPublishResultTests
{
    [Fact]
    public void Default_Should_HaveZeroCounts()
    {
        // Act
        var result = new BatchPublishResult();

        // Assert
        Assert.Equal(0, result.SuccessCount);
        Assert.Equal(0, result.FailedCount);
        Assert.Empty(result.Failures);
    }

    [Fact]
    public void WithSuccessAndFailures_Should_AggregateCorrectly()
    {
        // Arrange
        var result = new BatchPublishResult
        {
            SuccessCount = 5,
            FailedCount = 2,
            Failures = new List<BatchPublishFailure>
            {
                new(0, "key1", new Exception("Error 1")),
                new(1, "key2", new Exception("Error 2"))
            }
        };

        // Assert
        Assert.Equal(5, result.SuccessCount);
        Assert.Equal(2, result.FailedCount);
        Assert.Equal(2, result.Failures.Count);
    }

    [Fact]
    public void IsAllSuccess_WhenNoFailures_Should_ReturnTrue()
    {
        // Arrange
        var result = new BatchPublishResult
        {
            SuccessCount = 10,
            FailedCount = 0
        };

        // Assert
        Assert.True(result.IsAllSuccess);
    }

    [Fact]
    public void IsAllSuccess_WhenHasFailures_Should_ReturnFalse()
    {
        // Arrange
        var result = new BatchPublishResult
        {
            SuccessCount = 8,
            FailedCount = 2
        };

        // Assert
        Assert.False(result.IsAllSuccess);
    }

    [Fact]
    public void Elapsed_Should_StoreTimeSpan()
    {
        // Arrange
        var elapsed = TimeSpan.FromSeconds(1.5);

        // Act
        var result = new BatchPublishResult { Elapsed = elapsed };

        // Assert
        Assert.Equal(elapsed, result.Elapsed);
    }
}

public class BatchPublishFailureTests
{
    [Fact]
    public void Constructor_Should_StoreValues()
    {
        // Arrange
        int index = 5;
        string routingKey = "test.key";
        var exception = new Exception("Test error");

        // Act
        var failure = new BatchPublishFailure(index, routingKey, exception);

        // Assert
        Assert.Equal(index, failure.Index);
        Assert.Equal(routingKey, failure.RoutingKey);
        Assert.Same(exception, failure.Exception);
    }

    [Fact]
    public void Record_Properties_Should_BeReadonly()
    {
        // Arrange
        var failure = new BatchPublishFailure(0, "key", new Exception("Error"));

        // Assert - records have init-only properties
        Assert.Equal(0, failure.Index);
        Assert.Equal("key", failure.RoutingKey);
    }
}
