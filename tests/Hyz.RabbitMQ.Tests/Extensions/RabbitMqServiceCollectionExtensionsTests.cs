using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Extensions;
using Microsoft.Extensions.DependencyInjection;

namespace Hyz.RabbitMQ.Tests.Extensions;

public class RabbitMqServiceCollectionExtensionsTests
{
    [Fact]
    public void AddRabbitMq_WithDefaultName_Should_RegisterServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var connectionOptions = provider.GetService<RabbitMqConnectionOptions>();
        Assert.NotNull(connectionOptions);
        Assert.Equal("localhost", connectionOptions.HostName);

        var connectionManager = provider.GetService<IConnectionManager>();
        Assert.NotNull(connectionManager);
        Assert.True(connectionManager.Contains(ConnectionConstants.DefaultConnectionName));
    }

    [Fact]
    public void AddRabbitMq_WithCustomName_Should_RegisterWithCustomName()
    {
        // Arrange
        var services = new ServiceCollection();
        const string customName = "my-custom-connection";

        // Act
        services.AddRabbitMq(customName, options =>
        {
            options.HostName = "rabbitmq.local";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var connectionManager = provider.GetService<IConnectionManager>();
        Assert.NotNull(connectionManager);
        Assert.True(connectionManager.Contains(customName));
    }

    [Fact]
    public void AddRabbitMq_Should_RegisterIConnectionProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var connectionProvider = provider.GetService<IConnectionProvider>();
        Assert.NotNull(connectionProvider);
        Assert.Equal(ConnectionConstants.DefaultConnectionName, connectionProvider.Name);
    }

    [Fact]
    public void AddRabbitMq_Should_RegisterIPublisherService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var publisherService = provider.GetService<IPublisherService>();
        Assert.NotNull(publisherService);
    }

    [Fact]
    public void AddRabbitMq_Should_RegisterIConsumerService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var consumerService = provider.GetService<IConsumerService>();
        Assert.NotNull(consumerService);
    }

    [Fact]
    public void AddRabbitMq_Should_RegisterIMultiConnectionPublisherService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var multiPublisher = provider.GetService<IMultiConnectionPublisherService>();
        Assert.NotNull(multiPublisher);
    }

    [Fact]
    public void AddRabbitMq_Should_RegisterIMultiConnectionConsumerService()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var multiConsumer = provider.GetService<IMultiConnectionConsumerService>();
        Assert.NotNull(multiConsumer);
    }

    [Fact]
    public void AddRabbitMq_WithOptionsObject_Should_ApplyOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var options = new RabbitMqConnectionOptions
        {
            HostName = "custom-host",
            Port = 5673,
            UserName = "guest",
            Password = "secret"
        };

        // Act
        services.AddRabbitMq(options);

        // Assert
        var provider = services.BuildServiceProvider();
        var connectionOptions = provider.GetService<RabbitMqConnectionOptions>();
        Assert.NotNull(connectionOptions);
        Assert.Equal("custom-host", connectionOptions.HostName);
        Assert.Equal(5673, connectionOptions.Port);
        Assert.Equal("guest", connectionOptions.UserName);
    }

    [Fact]
    public void AddRabbitMq_DefaultConnection_Should_BeSet()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var connectionManager = provider.GetService<IConnectionManager>();
        Assert.NotNull(connectionManager);

        var defaultProvider = connectionManager.Default;
        Assert.NotNull(defaultProvider);
        Assert.Equal(ConnectionConstants.DefaultConnectionName, defaultProvider.Name);
    }
}

public class MultiConnectionConsumerServiceTests
{
    private static ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddRabbitMq(options =>
        {
            options.HostName = "localhost";
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public void GetConsumer_WithDefaultName_Should_ReturnConsumer()
    {
        var provider = BuildServiceProvider();
        var multiConsumer = provider.GetRequiredService<IMultiConnectionConsumerService>();

        var consumer = multiConsumer.GetConsumer(ConnectionConstants.DefaultConnectionName);

        Assert.NotNull(consumer);
    }

    [Fact]
    public void GetAllConsumers_Should_ReturnAtLeastDefaultConnection()
    {
        var provider = BuildServiceProvider();
        var multiConsumer = provider.GetRequiredService<IMultiConnectionConsumerService>();

        var consumers = multiConsumer.GetAllConsumers();

        Assert.NotEmpty(consumers);
        Assert.Contains(ConnectionConstants.DefaultConnectionName, consumers.Keys);
    }

    [Fact]
    public void GetConsumer_Should_ReturnConsumerForDefaultConnection()
    {
        var provider = BuildServiceProvider();
        var multiConsumer = provider.GetRequiredService<IMultiConnectionConsumerService>();

        var consumer = multiConsumer.GetConsumer(ConnectionConstants.DefaultConnectionName);

        Assert.NotNull(consumer);
    }
}

public class RabbitMqConnectionOptionsExtensionsTests
{
    [Fact]
    public void GetConnectionName_WithNameSet_Should_ReturnSetName()
    {
        var options = new RabbitMqConnectionOptions { Name = "custom-conn" };

        var name = options.GetConnectionName();

        Assert.Equal("custom-conn", name);
    }

    [Fact]
    public void GetConnectionName_WithNoName_Should_ReturnDefaultName()
    {
        var options = new RabbitMqConnectionOptions();

        var name = options.GetConnectionName();

        Assert.Equal(ConnectionConstants.DefaultConnectionName, name);
    }

    [Fact]
    public void GetConnectionName_WithHostAndPort_Should_ReturnFormattedName()
    {
        var options = new RabbitMqConnectionOptions
        {
            Name = "custom-host-port",
            HostName = "rabbitmq.local",
            Port = 5673
        };

        var name = options.GetConnectionName();

        Assert.Equal("custom-host-port", name);
    }

    [Fact]
    public void AddRabbitMq_MultipleConnections_Should_NotThrow()
    {
        var services = new ServiceCollection();
        var ex = Record.Exception(() =>
        {
            services.AddRabbitMq("conn-x", opts => opts.HostName = "host-x");
            services.AddRabbitMq("conn-y", opts => opts.HostName = "host-y");
        });

        Assert.Null(ex);
    }

    [Fact]
    public void AddRabbitMq_MultipleConnections_Should_ResolveMultiConsumerService()
    {
        var services = new ServiceCollection();
        services.AddRabbitMq("conn-x", opts => opts.HostName = "host-x");
        services.AddRabbitMq("conn-y", opts => opts.HostName = "host-y");
        var provider = services.BuildServiceProvider();

        var multiConsumer = provider.GetRequiredService<IMultiConnectionConsumerService>();

        Assert.NotNull(multiConsumer);
    }
}
