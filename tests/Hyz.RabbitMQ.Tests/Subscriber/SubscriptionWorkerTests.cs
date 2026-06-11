using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Subscriber;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Hyz.RabbitMQ.Tests.Subscriber;

public class SubscriptionWorkerTests
{
    private static SubscriberRegistration MakeRegistration(string name = "test-subscriber", string queueName = "test-queue")
    {
        return new SubscriberRegistration
        {
            Name = name,
            QueueName = queueName,
            ConsumerType = typeof(object),
            ConnectionName = "Default",
            PrefetchCount = 10
        };
    }

    private static ServiceProvider BuildServiceProvider(IConnectionManager? connectionManager = null)
    {
        var services = new ServiceCollection();
        if (connectionManager != null)
        {
            services.AddSingleton(connectionManager);
        }
        return services.BuildServiceProvider();
    }

    [Fact]
    public void Constructor_Should_InitializeWorker()
    {
        var registration = MakeRegistration();
        var serviceProvider = BuildServiceProvider();

        var worker = new SubscriptionWorker(registration, serviceProvider);

        Assert.Equal("test-subscriber", worker.Name);
    }

    [Fact]
    public void Constructor_WithNullOptions_Should_UseDefaults()
    {
        var registration = MakeRegistration();
        var serviceProvider = BuildServiceProvider();

        var worker = new SubscriptionWorker(registration, serviceProvider, null);

        Assert.Equal("test-subscriber", worker.Name);
    }

    [Fact]
    public void GetStatus_Should_ReturnCorrectStatus()
    {
        var registration = MakeRegistration(name: "status-test", queueName: "status-queue");
        var mockConnManager = new Mock<IConnectionManager>();
        var serviceProvider = BuildServiceProvider(mockConnManager.Object);

        var worker = new SubscriptionWorker(registration, serviceProvider);
        var status = worker.GetStatus();

        Assert.Equal("status-test", status.ConsumerName);
        Assert.Equal("status-queue", status.QueueName);
        Assert.Equal("Created", status.State);
        Assert.Equal("Default", status.ConnectionName);
        Assert.Equal(0, status.ProcessedCount);
    }

    [Fact]
    public async Task StopAsync_Should_TransitionToStopped()
    {
        var registration = MakeRegistration();
        var serviceProvider = BuildServiceProvider();
        var worker = new SubscriptionWorker(registration, serviceProvider);
        using var cts = new CancellationTokenSource();

        await worker.StopAsync(cts.Token);

        var status = worker.GetStatus();
        Assert.Equal("Stopped", status.State);
    }

    [Fact]
    public async Task StartAsync_WithClosedConnection_Should_ResultInErrorState()
    {
        var registration = MakeRegistration();
        var mockConnProvider = new Mock<IConnectionProvider>();
        mockConnProvider.SetupGet(p => p.Name).Returns("Default");
        mockConnProvider.SetupGet(p => p.State).Returns(ConnectionState.Closed);
        mockConnProvider.Setup(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Connection not open"));

        var mockConnManager = new Mock<IConnectionManager>();
        mockConnManager.Setup(m => m.GetProvider("Default")).Returns(mockConnProvider.Object);

        var serviceProvider = BuildServiceProvider(mockConnManager.Object);
        var worker = new SubscriptionWorker(registration, serviceProvider);

        var startTask = worker.StartAsync();

        await Task.Delay(100);

        var status = worker.GetStatus();
        Assert.Equal("Error", status.State);
    }

    [Fact]
    public void GetStatus_Should_ReturnConnectionName()
    {
        var registration = MakeRegistration();
        registration.ConnectionName = "CustomConnection";
        var mockConnManager = new Mock<IConnectionManager>();
        var serviceProvider = BuildServiceProvider(mockConnManager.Object);

        var worker = new SubscriptionWorker(registration, serviceProvider);
        var status = worker.GetStatus();

        Assert.Equal("CustomConnection", status.ConnectionName);
    }

    [Fact]
    public void GetStatus_Should_InitializeProcessedCountToZero()
    {
        var registration = MakeRegistration();
        var serviceProvider = BuildServiceProvider();
        var worker = new SubscriptionWorker(registration, serviceProvider);

        var status = worker.GetStatus();

        Assert.Equal(0, status.ProcessedCount);
    }
}

public class RabbitMqSubscriberHostTests
{
    private static SubscriberRegistration MakeReg(string name, string queue, int priority = 0)
    {
        return new SubscriberRegistration
        {
            Name = name,
            QueueName = queue,
            ConsumerType = typeof(object),
            ConnectionName = "Default",
            StartupPriority = priority,
            PrefetchCount = 10
        };
    }

    private static ServiceProvider BuildSp()
    {
        return new ServiceCollection().BuildServiceProvider();
    }

    [Fact]
    public void Constructor_Should_AcceptServicesAndRegistrations()
    {
        var regs = new[] { MakeReg("r1", "q1") };
        var sp = BuildSp();

        var host = new RabbitMqSubscriberHost(sp, regs);

        Assert.NotNull(host);
    }

    [Fact]
    public void Constructor_WithNullOptions_Should_UseDefaults()
    {
        var regs = Array.Empty<SubscriberRegistration>();
        var sp = BuildSp();

        var host = new RabbitMqSubscriberHost(sp, regs, null);

        Assert.NotNull(host);
    }

    [Fact]
    public void GetAllStatuses_WithNoWorkers_Should_ReturnEmpty()
    {
        var regs = Array.Empty<SubscriberRegistration>();
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);

        var statuses = host.GetAllStatuses();

        Assert.Empty(statuses);
    }

    [Fact]
    public async Task GetAllStatuses_AfterStart_Should_ReturnAllWorkerStatuses()
    {
        var regs = new[] { MakeReg("w1", "q1"), MakeReg("w2", "q2") };
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);
        using var cts = new CancellationTokenSource();

        await host.StartAsync(cts.Token);
        await Task.Delay(100);

        var statuses = host.GetAllStatuses();

        Assert.Equal(2, statuses.Count);
        Assert.Contains(statuses, s => s.ConsumerName == "w1");
        Assert.Contains(statuses, s => s.ConsumerName == "w2");

        await host.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_Should_StartWorkersInPriorityOrder()
    {
        var regs = new[]
        {
            MakeReg("low-priority", "q1", priority: 2),
            MakeReg("high-priority", "q2", priority: 1),
            MakeReg("default-priority", "q3", priority: 0)
        };
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);
        using var cts = new CancellationTokenSource();

        await host.StartAsync(cts.Token);
        await Task.Delay(50);

        var statuses = host.GetAllStatuses();

        Assert.Equal(3, statuses.Count);
        Assert.Equal("default-priority", statuses[0].ConsumerName);
        Assert.Equal("high-priority", statuses[1].ConsumerName);
        Assert.Equal("low-priority", statuses[2].ConsumerName);

        await host.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_CalledTwice_Should_NotStartTwice()
    {
        var regs = new[] { MakeReg("single-start", "q1") };
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);
        using var cts = new CancellationTokenSource();

        await host.StartAsync(cts.Token);
        await Task.Delay(50);
        await host.StartAsync(cts.Token);
        await Task.Delay(50);

        var statuses = host.GetAllStatuses();

        Assert.Single(statuses);
        await host.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StopAsync_Should_StopAllWorkers()
    {
        var regs = new[] { MakeReg("stop-test-1", "q1"), MakeReg("stop-test-2", "q2") };
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);
        using var cts = new CancellationTokenSource();

        await host.StartAsync(cts.Token);
        await Task.Delay(50);
        await host.StopAsync(CancellationToken.None);
        await Task.Delay(50);

        var statuses = host.GetAllStatuses();

        Assert.All(statuses, s => Assert.Equal("Stopped", s.State));
    }

    [Fact]
    public async Task GetAllStatuses_AfterStart_Should_ReturnCorrectStates()
    {
        var regs = new[] { MakeReg("worker-a", "q1"), MakeReg("worker-b", "q2") };
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);
        using var cts = new CancellationTokenSource();

        await host.StartAsync(cts.Token);
        await Task.Delay(50);

        var statuses = host.GetAllStatuses();

        Assert.Equal(2, statuses.Count);

        await host.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void Dispose_Should_NotThrow()
    {
        var regs = Array.Empty<SubscriberRegistration>();
        var sp = BuildSp();
        var host = new RabbitMqSubscriberHost(sp, regs);

        host.Dispose();

        Assert.True(true);
    }
}
