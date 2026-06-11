using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class HandleResultTests
{
    [Fact]
    public void SuccessResult_Should_IndicateSuccess()
    {
        // Act
        var result = HandleResult.SuccessResult;

        // Assert
        Assert.True(result.IsSuccess);
        Assert.False(result.Requeue);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Reject_Should_IndicateFailure()
    {
        // Arrange
        string errorMessage = "Something went wrong";

        // Act
        var result = HandleResult.Reject(errorMessage);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Requeue);
        Assert.Equal(errorMessage, result.ErrorMessage);
    }

    [Fact]
    public void Reject_WithNullMessage_Should_Work()
    {
        // Act
        var result = HandleResult.Reject(null);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Requeue);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Retry_Should_IndicateRetry()
    {
        // Arrange
        string errorMessage = "Temporary failure";

        // Act
        var result = HandleResult.Retry(errorMessage);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.True(result.Requeue);
        Assert.Equal(errorMessage, result.ErrorMessage);
        Assert.Equal(1, result.RetryCount);
    }

    [Fact]
    public void Retry_WithCustomRetryCount_Should_UseProvidedCount()
    {
        // Act
        var result = HandleResult.Retry("error", 3);

        // Assert
        Assert.Equal(3, result.RetryCount);
    }

    [Fact]
    public void Retry_WithNullMessage_Should_Work()
    {
        // Act
        var result = HandleResult.Retry(null);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.True(result.Requeue);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void SuccessResult_Requeue_Should_BeFalse()
    {
        // Act
        var result = HandleResult.SuccessResult;

        // Assert
        Assert.False(result.Requeue);
    }

    [Fact]
    public void SuccessResult_ErrorMessage_Should_BeNull()
    {
        // Act
        var result = HandleResult.SuccessResult;

        // Assert
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void SuccessResult_RetryCount_Should_BeZero()
    {
        // Act
        var result = HandleResult.SuccessResult;

        // Assert
        Assert.Equal(0, result.RetryCount);
    }
}
