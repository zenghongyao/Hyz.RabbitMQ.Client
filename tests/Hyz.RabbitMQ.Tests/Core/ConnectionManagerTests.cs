using Hyz.RabbitMQ.Core;
using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hyz.RabbitMQ.Tests.Core;

public class ConnectionManagerTests
{
    [Fact]
    public void Default_WhenNoConnections_Should_ThrowInvalidOperationException()
    {
        // Arrange
        var manager = new ConnectionManager();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => manager.Default);
        Assert.Contains("No default connection registered", exception.Message);
    }

    [Fact]
    public void Register_WithSingleProvider_Should_SetAsDefault()
    {
        // Arrange
        var manager = new ConnectionManager();
        var mockProvider = new MockConnectionProvider("test-connection");

        // Act
        manager.Register(mockProvider);

        // Assert
        Assert.Same(mockProvider, manager.Default);
    }

    [Fact]
    public void Register_SecondProviderWithIsDefaultTrue_Should_NotChangeDefault()
    {
        // Note: The current implementation only sets default when _defaultProvider is null
        // So even with isDefault=true, if a default is already set, it won't change
        var manager = new ConnectionManager();
        var provider1 = new MockConnectionProvider("conn-1");
        var provider2 = new MockConnectionProvider("conn-2");

        // Act
        manager.Register(provider1, isDefault: true);
        manager.Register(provider2, isDefault: true);

        // Assert - first provider remains default due to ??= assignment
        Assert.Same(provider1, manager.Default);
    }

    [Fact]
    public void Register_FirstProvider_Should_BeDefault()
    {
        // Arrange
        var manager = new ConnectionManager();
        var provider1 = new MockConnectionProvider("conn-1");
        var provider2 = new MockConnectionProvider("conn-2");

        // Act - register without specifying default
        manager.Register(provider1);
        manager.Register(provider2);

        // Assert - first one is default
        Assert.Same(provider1, manager.Default);
    }

    [Fact]
    public void Register_WithDuplicateName_Should_Be_Idempotent()
    {
        // Arrange
        var manager = new ConnectionManager();
        var provider1 = new MockConnectionProvider("same-name");
        var provider2 = new MockConnectionProvider("same-name");

        manager.Register(provider1);

        // Act - duplicate registration should NOT throw
        var exception = Record.Exception(() => manager.Register(provider2));

        // Assert - no exception, original provider preserved
        Assert.Null(exception);
        Assert.Same(provider1, manager.GetProvider("same-name"));
        Assert.NotSame(provider2, manager.GetProvider("same-name"));
    }

    [Fact]
    public void GetProvider_WithExistingName_Should_ReturnProvider()
    {
        // Arrange
        var manager = new ConnectionManager();
        var provider = new MockConnectionProvider("test-provider");
        manager.Register(provider);

        // Act
        var result = manager.GetProvider("test-provider");

        // Assert
        Assert.Same(provider, result);
    }

    [Fact]
    public void GetProvider_WithNonExistingName_Should_ThrowKeyNotFoundException()
    {
        // Arrange
        var manager = new ConnectionManager();

        // Act & Assert
        var exception = Assert.Throws<KeyNotFoundException>(() => manager.GetProvider("non-existing"));
        Assert.Contains("not found", exception.Message);
    }

    [Fact]
    public void Contains_WithExistingName_Should_ReturnTrue()
    {
        // Arrange
        var manager = new ConnectionManager();
        var provider = new MockConnectionProvider("my-connection");
        manager.Register(provider);

        // Act & Assert
        Assert.True(manager.Contains("my-connection"));
    }

    [Fact]
    public void Contains_WithNonExistingName_Should_ReturnFalse()
    {
        // Arrange
        var manager = new ConnectionManager();

        // Act & Assert
        Assert.False(manager.Contains("non-existing"));
    }

    [Fact]
    public void GetAllConnectionNames_Should_ReturnAllNames()
    {
        // Arrange
        var manager = new ConnectionManager();
        manager.Register(new MockConnectionProvider("conn-1"));
        manager.Register(new MockConnectionProvider("conn-2"));
        manager.Register(new MockConnectionProvider("conn-3"));

        // Act
        var names = manager.GetAllConnectionNames();

        // Assert
        Assert.Equal(3, names.Count);
        Assert.Contains("conn-1", names);
        Assert.Contains("conn-2", names);
        Assert.Contains("conn-3", names);
    }

    private class MockConnectionProvider : IConnectionProvider
    {
        public string Name { get; }
        public ConnectionState State => ConnectionState.Open;
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
            throw new NotImplementedException();
        }

        public global::RabbitMQ.Client.IConnection GetConnection()
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryReconnectAsync()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
        }

        public IExchangeManager ExchangeManager => null!;
        public IQueueManager QueueManager => null!;

        public int RentedChannelCount => 0;
        public int PooledChannelCount => 0;

        public bool IsHealthy() => true;

        public void Dispose()
        {
        }
    }
}
