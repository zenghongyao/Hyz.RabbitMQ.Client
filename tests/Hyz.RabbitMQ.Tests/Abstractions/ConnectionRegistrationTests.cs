using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class ConnectionRegistrationTests
{
    [Fact]
    public void WithRequiredProperties_Should_SetProperties()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions { HostName = "rabbitmq.local" };

        // Act
        var registration = new ConnectionRegistration
        {
            Name = "conn-local",
            Options = options,
            IsDefault = true
        };

        // Assert
        Assert.Equal("conn-local", registration.Name);
        Assert.Same(options, registration.Options);
        Assert.True(registration.IsDefault);
    }

    [Fact]
    public void IsDefault_False_Should_BeDefault()
    {
        // Arrange & Act
        var registration = new ConnectionRegistration
        {
            Name = "secondary",
            Options = new RabbitMqConnectionOptions(),
            IsDefault = false
        };

        // Assert
        Assert.False(registration.IsDefault);
    }

    [Fact]
    public void Options_Should_BeAccessible()
    {
        // Arrange
        var options = new RabbitMqConnectionOptions
        {
            HostName = "mq.example.com",
            Port = 5673,
            UserName = "guest",
            Password = "secret"
        };

        // Act
        var registration = new ConnectionRegistration
        {
            Name = "guest-conn",
            Options = options,
            IsDefault = false
        };

        // Assert
        Assert.Equal("mq.example.com", registration.Options.HostName);
        Assert.Equal(5673, registration.Options.Port);
        Assert.Equal("guest", registration.Options.UserName);
        Assert.Equal("secret", registration.Options.Password);
    }

    [Fact]
    public void MultipleRegistrations_Should_BeIndependent()
    {
        // Arrange
        var options1 = new RabbitMqConnectionOptions { HostName = "host1" };
        var options2 = new RabbitMqConnectionOptions { HostName = "host2" };

        // Act
        var reg1 = new ConnectionRegistration { Name = "conn-1", Options = options1 };
        var reg2 = new ConnectionRegistration { Name = "conn-2", Options = options2 };

        // Assert
        Assert.NotSame(reg1, reg2);
        Assert.NotSame(reg1.Options, reg2.Options);
        Assert.Equal("host1", reg1.Options.HostName);
        Assert.Equal("host2", reg2.Options.HostName);
    }
}
