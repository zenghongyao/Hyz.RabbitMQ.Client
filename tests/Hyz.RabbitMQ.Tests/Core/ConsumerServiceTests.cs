using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Abstractions;
using Moq;

namespace Hyz.RabbitMQ.Tests.Core;

public class ConsumerServiceTests
{
    private readonly MockConnectionProvider _mockProvider;
    private readonly ConsumerService _consumerService;

    public ConsumerServiceTests()
    {
        _mockProvider = new MockConnectionProvider("test-connection");
        _consumerService = new ConsumerService(_mockProvider);
    }

    [Fact]
    public void Constructor_Should_SetConnectionName()
    {
        // Assert
        Assert.Equal("test-connection", _consumerService.ConnectionName);
    }

    [Fact]
    public void Constructor_WithNullLogger_Should_UseNullLogger()
    {
        // Arrange
        var service = new ConsumerService(_mockProvider, null);

        // Assert - should not throw
        Assert.Equal("test-connection", service.ConnectionName);
    }

    [Fact]
    public async Task StartConsumingAsync_Should_ThrowWhenConnectionNotOpen()
    {
        // Arrange
        var mockHandler = new Mock<IMessageHandler>();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _consumerService.StartConsumingAsync("test-queue", mockHandler.Object));
        Assert.Contains("not open", exception.Message);
    }

    [Fact]
    public async Task StopConsuming_WithNonExistentTag_Should_NotThrow()
    {
        // Act & Assert - should not throw
        await _consumerService.StopConsumingAsync("non-existent-tag");
    }

    [Fact]
    public void Dispose_Should_NotThrow()
    {
        // Arrange
        var service = new ConsumerService(_mockProvider);

        // Act & Assert - should not throw
        service.Dispose();
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
