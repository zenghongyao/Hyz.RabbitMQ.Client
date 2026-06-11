using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Core;

public class RabbitMqConnectionOptionsTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions();

        // Assert
        Assert.Equal("localhost", options.HostName);
        Assert.Equal(5672, options.Port);
        Assert.Equal("guest", options.UserName);
        Assert.Equal("guest", options.Password);
        Assert.Equal("/", options.VirtualHost);
        Assert.Equal(30000, options.ConnectionTimeout);
        Assert.Equal((ushort)60, options.Heartbeat);
        Assert.Equal(5000, options.RetryDelayMs);
        Assert.True(options.AutoReconnect);
        Assert.Null(options.Name);
    }

    [Fact]
    public void WithHostName_Should_SetHostName()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { HostName = "rabbitmq.local" };

        // Assert
        Assert.Equal("rabbitmq.local", options.HostName);
    }

    [Fact]
    public void WithPort_Should_SetPort()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { Port = 5673 };

        // Assert
        Assert.Equal(5673, options.Port);
    }

    [Fact]
    public void WithCredentials_Should_SetUserAndPassword()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions
        {
            UserName = "guest",
            Password = "secret"
        };

        // Assert
        Assert.Equal("guest", options.UserName);
        Assert.Equal("secret", options.Password);
    }

    [Fact]
    public void WithVirtualHost_Should_SetVirtualHost()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { VirtualHost = "/production" };

        // Assert
        Assert.Equal("/production", options.VirtualHost);
    }

    [Fact]
    public void WithAutoReconnect_Should_SetAutoReconnect()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { AutoReconnect = false };

        // Assert
        Assert.False(options.AutoReconnect);
    }

    [Fact]
    public void GetConnectionName_WithNameSet_Should_ReturnName()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions
        {
            Name = "MyApp-Connection"
        };

        // Act
        var name = options.GetConnectionName();

        // Assert
        Assert.Equal("MyApp-Connection", name);
    }

    [Fact]
    public void GetConnectionName_WithNoName_Should_ReturnDefaultConnectionName()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions();

        // Act
        var name = options.GetConnectionName();

        // Assert
        Assert.Equal(ConnectionConstants.DefaultConnectionName, name);
    }

    [Fact]
    public void WithMaxRetryCount_Should_SetMaxRetryCount()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { MaxRetryCount = 5 };

        // Assert
        Assert.Equal(5, options.MaxRetryCount);
    }

    [Fact]
    public void WithEnableTls_Should_SetEnableTls()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { EnableTls = true };

        // Assert
        Assert.True(options.EnableTls);
    }
}
