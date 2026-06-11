using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class BatchHandleResultTests
{
    [Fact]
    public void Default_Should_HaveEmptyCollections()
    {
        // Act
        var result = new BatchHandleResult();

        // Assert
        Assert.Equal(0, result.SuccessCount);
        Assert.Empty(result.RetryIndices);
        Assert.Empty(result.RejectIndices);
    }

    [Fact]
    public void AllSuccess_Should_CreateSuccessResult()
    {
        // Arrange
        int count = 10;

        // Act
        var result = BatchHandleResult.AllSuccess(count);

        // Assert
        Assert.Equal(count, result.SuccessCount);
        Assert.Empty(result.RetryIndices);
        Assert.Empty(result.RejectIndices);
    }

    [Fact]
    public void Partial_Should_StoreIndices()
    {
        // Arrange
        var retryIndices = new[] { 1, 3, 5 };
        var rejectIndices = new[] { 2, 4 };

        // Act
        var result = BatchHandleResult.Partial(8, retryIndices, rejectIndices);

        // Assert
        Assert.Equal(8, result.SuccessCount);
        Assert.Equal(3, result.RetryIndices.Count);
        Assert.Equal(2, result.RejectIndices.Count);
    }

    [Fact]
    public void Partial_WithNullIndices_Should_UseEmptyLists()
    {
        // Act
        var result = BatchHandleResult.Partial(5, null, null);

        // Assert
        Assert.Equal(5, result.SuccessCount);
        Assert.Empty(result.RetryIndices);
        Assert.Empty(result.RejectIndices);
    }

    [Fact]
    public void RetryIndices_Should_BeReadOnly()
    {
        // Arrange
        var result = BatchHandleResult.Partial(5, new[] { 1, 2 }, null);

        // Assert
        Assert.True(result.RetryIndices is IReadOnlyList<int>);
    }

    [Fact]
    public void RejectIndices_Should_BeReadOnly()
    {
        // Arrange
        var result = BatchHandleResult.Partial(5, null, new[] { 0, 1 });

        // Assert
        Assert.True(result.RejectIndices is IReadOnlyList<int>);
    }
}
