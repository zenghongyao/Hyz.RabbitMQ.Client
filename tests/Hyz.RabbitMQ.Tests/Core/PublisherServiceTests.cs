using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hyz.RabbitMQ.Tests.Core;

public class PublisherServiceTests
{
    private readonly MockConnectionProvider _mockProvider;
    private readonly PublisherService _publisherService;

    public PublisherServiceTests()
    {
        _mockProvider = new MockConnectionProvider("test-connection");
        _publisherService = new PublisherService(_mockProvider);
    }

    [Fact]
    public void Constructor_Should_SetConnectionName()
    {
        // Assert
        Assert.Equal("test-connection", _publisherService.ConnectionName);
    }

    [Fact]
    public void Constructor_WithNullLogger_Should_UseNullLogger()
    {
        // Arrange
        var service = new PublisherService(_mockProvider, null);

        // Assert - should not throw
        Assert.Equal("test-connection", service.ConnectionName);
    }

    [Fact]
    public async Task PublishAsync_Should_ThrowWhenConnectionNotOpen()
    {
        // Arrange
        var message = MessageBody.FromString("test message");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _publisherService.PublishAsync("test-queue", message));
    }

    [Fact]
    public async Task PublishToExchangeAsync_Should_ThrowWhenConnectionNotOpen()
    {
        // Arrange
        var message = MessageBody.FromString("test message");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _publisherService.PublishToExchangeAsync("exchange", "routing-key", message));
    }

    [Fact]
    public async Task PublishWithConfirmationAsync_Should_ReturnFailureWhenConnectionNotOpen()
    {
        // Arrange
        var message = MessageBody.FromString("test message");

        // Act
        var result = await _publisherService.PublishWithConfirmationAsync("exchange", "routing-key", message);

        // Assert - should return failure result, not throw
        Assert.False(result.IsSuccess);
        Assert.NotNull(result.ErrorMessage);
    }

    [Fact]
    public async Task PublishBatchAsync_WithMessages_Should_ThrowWhenConnectionNotOpen()
    {
        // Arrange
        var messages = new List<MessageBody>
        {
            MessageBody.FromString("msg1"),
            MessageBody.FromString("msg2")
        };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _publisherService.PublishBatchAsync("exchange", "routing-key", messages));
    }

    private class MockConnectionProvider : IConnectionProvider
    {
        public string Name { get; }
        public ConnectionState State => ConnectionState.Closed;
        public event EventHandler<ConnectionEventArgs>? ConnectionShutdown;
        public event EventHandler<ConnectionEventArgs>? ConnectionRecovered;
        public IChannelPool? ChannelPool => null;
        public DateTimeOffset? ConnectedAt => null;

        public MockConnectionProvider(string name)
        {
            Name = name;
        }

        public Task<global::RabbitMQ.Client.IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            throw new InvalidOperationException("Connection is not open");
        }

        public global::RabbitMQ.Client.IConnection GetConnection()
        {
            throw new InvalidOperationException("Connection is not open");
        }

        public Task<bool> TryReconnectAsync()
        {
            throw new NotImplementedException();
        }

        public bool IsHealthy() => false;

        public void Close()
        {
        }

        public IExchangeManager ExchangeManager => null!;
        public IQueueManager QueueManager => null!;

        public int RentedChannelCount => 0;
        public int PooledChannelCount => 0;

        public void Dispose()
        {
        }
    }
}
