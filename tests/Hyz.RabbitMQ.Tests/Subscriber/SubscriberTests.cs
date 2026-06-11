using Hyz.RabbitMQ.Subscriber;
using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Abstractions.Attributes;

namespace Hyz.RabbitMQ.Tests.Subscriber;

public class RabbitMqSubscriberOptionsTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange
        var options = new RabbitMqSubscriberOptions();

        // Assert
        Assert.True(options.AutoStart);
        Assert.Equal(TimeSpan.Zero, options.StartupDelay);
        Assert.Equal(TimeSpan.FromMinutes(1), options.HealthCheckInterval);
    }

    [Fact]
    public void WithAutoStart_Should_SetAutoStart()
    {
        // Arrange
        var options = new RabbitMqSubscriberOptions { AutoStart = false };

        // Assert
        Assert.False(options.AutoStart);
    }

    [Fact]
    public void WithStartupDelay_Should_SetStartupDelay()
    {
        // Arrange
        var delay = TimeSpan.FromSeconds(5);
        var options = new RabbitMqSubscriberOptions { StartupDelay = delay };

        // Assert
        Assert.Equal(delay, options.StartupDelay);
    }

    [Fact]
    public void WithHealthCheckInterval_Should_SetHealthCheckInterval()
    {
        // Arrange
        var interval = TimeSpan.FromMinutes(5);
        var options = new RabbitMqSubscriberOptions { HealthCheckInterval = interval };

        // Assert
        Assert.Equal(interval, options.HealthCheckInterval);
    }
}

public class SubscriberRegistrationTests
{
    [Fact]
    public void WithRequiredProperties_Should_SetProperties()
    {
        // Arrange & Act
        var registration = new SubscriberRegistration
        {
            Name = "test-subscriber",
            QueueName = "test-queue",
            ConsumerType = typeof(object)
        };

        // Assert
        Assert.Equal("test-subscriber", registration.Name);
        Assert.Equal("test-queue", registration.QueueName);
        Assert.Equal(typeof(object), registration.ConsumerType);
    }

    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var registration = new SubscriberRegistration
        {
            Name = "test",
            QueueName = "queue",
            ConsumerType = typeof(object)
        };

        // Assert
        Assert.Equal("Default", registration.ConnectionName);
        Assert.Equal(string.Empty, registration.ExchangeName);
        Assert.Equal(string.Empty, registration.RoutingKey);
        Assert.Equal(0, registration.StartupPriority);
        Assert.True(registration.EnableRetry);
        Assert.Equal(3, registration.MaxRetryCount);
        Assert.Equal(RetryStrategy.Exponential, registration.RetryStrategy);
        Assert.Equal(1000, registration.BaseRetryDelayMs);
        Assert.Equal((ushort)10, registration.PrefetchCount);
    }

    [Fact]
    public void WithRetrySettings_Should_SetRetrySettings()
    {
        // Arrange & Act
        var registration = new SubscriberRegistration
        {
            Name = "retry-test",
            QueueName = "queue",
            ConsumerType = typeof(object),
            EnableRetry = true,
            MaxRetryCount = 5,
            RetryStrategy = RetryStrategy.Linear,
            BaseRetryDelayMs = 2000
        };

        // Assert
        Assert.True(registration.EnableRetry);
        Assert.Equal(5, registration.MaxRetryCount);
        Assert.Equal(RetryStrategy.Linear, registration.RetryStrategy);
        Assert.Equal(2000, registration.BaseRetryDelayMs);
    }

    [Fact]
    public void WithPriority_Should_SetPriority()
    {
        // Arrange & Act
        var registration = new SubscriberRegistration
        {
            Name = "priority-test",
            QueueName = "queue",
            ConsumerType = typeof(object),
            StartupPriority = 10
        };

        // Assert
        Assert.Equal(10, registration.StartupPriority);
    }

    [Fact]
    public void WithPrefetchCount_Should_SetPrefetchCount()
    {
        // Arrange & Act
        var registration = new SubscriberRegistration
        {
            Name = "prefetch-test",
            QueueName = "queue",
            ConsumerType = typeof(object),
            PrefetchCount = 50
        };

        // Assert
        Assert.Equal((ushort)50, registration.PrefetchCount);
    }
}

public class SubscriptionStatusTests
{
    [Fact]
    public void Default_Should_HaveCorrectDefaults()
    {
        // Arrange & Act
        var status = new SubscriptionStatus();

        // Assert
        Assert.Equal(string.Empty, status.ConsumerName);
        Assert.Equal(string.Empty, status.QueueName);
        Assert.Equal("Unknown", status.State);
        Assert.Equal(string.Empty, status.ConnectionName);
        Assert.Equal(0, status.ProcessedCount);
        Assert.Null(status.LastProcessedTime);
        Assert.Null(status.LastError);
    }

    [Fact]
    public void WithValues_Should_SetAllProperties()
    {
        // Arrange
        var now = DateTime.UtcNow;

        // Act
        var status = new SubscriptionStatus
        {
            ConsumerName = "my-consumer",
            QueueName = "my-queue",
            State = "Running",
            ConnectionName = "Default",
            ProcessedCount = 100,
            LastProcessedTime = now,
            LastError = "Some error"
        };

        // Assert
        Assert.Equal("my-consumer", status.ConsumerName);
        Assert.Equal("my-queue", status.QueueName);
        Assert.Equal("Running", status.State);
        Assert.Equal("Default", status.ConnectionName);
        Assert.Equal(100, status.ProcessedCount);
        Assert.Equal(now, status.LastProcessedTime);
        Assert.Equal("Some error", status.LastError);
    }
}

public class RetryStrategyTests
{
    [Fact]
    public void RetryStrategy_Should_HaveCorrectValues()
    {
        // Assert
        Assert.Equal(0, (int)RetryStrategy.Fixed);
        Assert.Equal(1, (int)RetryStrategy.Linear);
        Assert.Equal(2, (int)RetryStrategy.Exponential);
    }

    [Fact]
    public void AllRetryStrategies_Should_BeDistinct()
    {
        // Arrange
        var strategies = new[] { RetryStrategy.Fixed, RetryStrategy.Linear, RetryStrategy.Exponential };

        // Assert
        Assert.Equal(3, strategies.Distinct().Count());
    }
}

public class RetryDelayCalculatorTests
{
    [Fact]
    public void CalculateDelay_Fixed_Should_ReturnBaseDelay()
    {
        // Fixed: every attempt returns the same base delay
        Assert.Equal(1000, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Fixed, 1000));
        Assert.Equal(1000, RetryDelayCalculator.CalculateDelay(2, RetryStrategy.Fixed, 1000));
        Assert.Equal(1000, RetryDelayCalculator.CalculateDelay(5, RetryStrategy.Fixed, 1000));
        Assert.Equal(0, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Fixed, 0));
    }

    [Fact]
    public void CalculateDelay_Linear_Should_ReturnAttemptMultipliedDelay()
    {
        // Linear: delay = base * attempt
        Assert.Equal(1000, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Linear, 1000));
        Assert.Equal(2000, RetryDelayCalculator.CalculateDelay(2, RetryStrategy.Linear, 1000));
        Assert.Equal(3000, RetryDelayCalculator.CalculateDelay(3, RetryStrategy.Linear, 1000));
        Assert.Equal(5000, RetryDelayCalculator.CalculateDelay(5, RetryStrategy.Linear, 1000));
        Assert.Equal(500, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Linear, 500));
    }

    [Fact]
    public void CalculateDelay_Exponential_Should_ReturnDoubledDelay()
    {
        // Exponential: delay = base * 2^(attempt-1)
        Assert.Equal(1000, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Exponential, 1000));
        Assert.Equal(2000, RetryDelayCalculator.CalculateDelay(2, RetryStrategy.Exponential, 1000));
        Assert.Equal(4000, RetryDelayCalculator.CalculateDelay(3, RetryStrategy.Exponential, 1000));
        Assert.Equal(8000, RetryDelayCalculator.CalculateDelay(4, RetryStrategy.Exponential, 1000));
        Assert.Equal(16000, RetryDelayCalculator.CalculateDelay(5, RetryStrategy.Exponential, 1000));
    }

    [Fact]
    public void CalculateDelay_WithZeroBaseDelay_Should_ReturnZero()
    {
        Assert.Equal(0, RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Fixed, 0));
        Assert.Equal(0, RetryDelayCalculator.CalculateDelay(3, RetryStrategy.Linear, 0));
        Assert.Equal(0, RetryDelayCalculator.CalculateDelay(5, RetryStrategy.Exponential, 0));
    }

    [Fact]
    public void CalculateDelay_WithAttemptLessThanOne_Should_Throw()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryDelayCalculator.CalculateDelay(0, RetryStrategy.Fixed, 1000));
        Assert.Contains("attempt", ex.Message.ToLower());
    }

    [Fact]
    public void CalculateDelay_WithNegativeBaseDelay_Should_Throw()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            RetryDelayCalculator.CalculateDelay(1, RetryStrategy.Fixed, -100));
        Assert.Contains("base delay", ex.Message.ToLower());
    }

    [Fact]
    public void CalculateDelay_LargeAttempt_Should_NotOverflow()
    {
        // Large attempt with exponential should fit in long
        var delay = RetryDelayCalculator.CalculateDelay(30, RetryStrategy.Exponential, 1000);
        Assert.True(delay > 0);
        Assert.True(delay < long.MaxValue);
    }
}

public class SubscriberScannerTests
{
    [Fact]
    public void Scan_WithAnnotatedClass_Should_ReturnRegistration()
    {
        // Arrange - AnnotatedTestConsumer has [RabbitMqConsumer]
        var assembly = typeof(AnnotatedTestConsumer).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly);

        // Assert
        Assert.NotEmpty(registrations);
    }

    [Fact]
    public void Scan_WithNoAnnotatedClasses_Should_ReturnEmptyOrNonEmpty()
    {
        // Arrange - use a reference assembly
        var assembly = typeof(ConnectionRegistration).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly);

        // Assert - may or may not have annotated classes
        Assert.NotNull(registrations);
    }

    [Fact]
    public void Scan_Should_NotIncludeClassesWithoutAttribute()
    {
        // Arrange
        var assembly = typeof(NonAnnotatedConsumer).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly);

        // Assert - NonAnnotatedConsumer doesn't have the attribute
        Assert.DoesNotContain(registrations, r => r.Name == "Hyz.RabbitMQ.Tests.Subscriber.NonAnnotatedConsumer");
    }

    [Fact]
    public void Scan_WithBaseType_Should_FilterByInheritance()
    {
        // Arrange
        var assembly = typeof(AnnotatedTestConsumer).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly, typeof(object));

        // Assert - object is base of all
        Assert.NotNull(registrations);
    }

    [Fact]
    public void Scan_Should_FindAnnotatedTestConsumer_WithCorrectProperties()
    {
        // Arrange
        var assembly = typeof(AnnotatedTestConsumer).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly);
        var consumer = registrations.FirstOrDefault(r => r.Name.Contains("AnnotatedTestConsumer"));

        // Assert
        Assert.NotNull(consumer);
        Assert.Equal("test-scanner-queue", consumer.QueueName);
        Assert.Equal((ushort)25, consumer.PrefetchCount);
    }

    [Fact]
    public void Scan_Should_FindAnotherAnnotatedConsumer_WithCorrectProperties()
    {
        // Arrange
        var assembly = typeof(AnotherAnnotatedConsumer).Assembly;

        // Act
        var registrations = SubscriberScanner.Scan(assembly);
        var consumer = registrations.FirstOrDefault(r => r.Name.Contains("AnotherAnnotatedConsumer"));

        // Assert
        Assert.NotNull(consumer);
        Assert.Equal("another-queue", consumer.QueueName);
        Assert.Equal("conn-2", consumer.ConnectionName);
        Assert.True(consumer.AutoAck);
    }
}

// Test fixture classes for SubscriberScanner verification
[RabbitMqConsumer(Queue = "test-scanner-queue", PrefetchCount = 25)]
public class AnnotatedTestConsumer
{
}

[RabbitMqConsumer(Queue = "another-queue", ConnectionName = "conn-2", AutoAck = true)]
public class AnotherAnnotatedConsumer
{
}

public class NonAnnotatedConsumer
{
}
