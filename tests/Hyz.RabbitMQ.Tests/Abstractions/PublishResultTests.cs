using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class PublishResultTests
{
    [Fact]
    public void Success_Should_ReturnSuccessResult()
    {
        // Arrange
        ulong sequenceNumber = 123ul;

        // Act
        var result = PublishResult.Success(sequenceNumber);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.Equal(sequenceNumber, result.SequenceNumber);
        Assert.Null(result.ErrorMessage);
        Assert.NotNull(result.ConfirmedAt);
    }

    [Fact]
    public void Failure_Should_ReturnFailureResult()
    {
        // Arrange
        string errorMessage = "Publish failed";

        // Act
        var result = PublishResult.Failure(errorMessage);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.Equal(0ul, result.SequenceNumber);
        Assert.Equal(errorMessage, result.ErrorMessage);
    }

    [Fact]
    public void SuccessResult_IsSuccess_Should_BeTrue()
    {
        // Act
        var result = PublishResult.Success(0);

        // Assert
        Assert.True(result.IsSuccess);
    }

    [Fact]
    public void FailureResult_IsSuccess_Should_BeFalse()
    {
        // Act
        var result = PublishResult.Failure("error");

        // Assert
        Assert.False(result.IsSuccess);
    }

    [Fact]
    public void SuccessResult_ConfirmedAt_Should_NotBeNull()
    {
        // Act
        var result = PublishResult.Success(0);

        // Assert
        Assert.NotNull(result.ConfirmedAt);
    }

    [Fact]
    public void FailureResult_ConfirmedAt_Should_BeNull()
    {
        // Act
        var result = PublishResult.Failure("error");

        // Assert
        Assert.Null(result.ConfirmedAt);
    }
}
