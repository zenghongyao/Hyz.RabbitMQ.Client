using Hyz.RabbitMQ.Serialization;

namespace Hyz.RabbitMQ.Tests.Serialization;

public class SystemTextJsonSerializerTests
{
    private readonly SystemTextJsonSerializer _serializer = new();

    [Fact]
    public void ContentType_Should_ReturnApplicationJson()
    {
        Assert.Equal("application/json", _serializer.ContentType);
    }

    [Fact]
    public void Serialize_WithSimpleObject_Should_ReturnBytes()
    {
        // Arrange
        var obj = new TestMessage { Id = 1, Name = "Test" };

        // Act
        var result = _serializer.Serialize(obj);

        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void Deserialize_WithValidJson_Should_ReturnObject()
    {
        // Arrange
        var json = "{\"id\":42,\"name\":\"Hello\"}";
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);

        // Act
        var result = _serializer.Deserialize<TestMessage>(bytes);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(42, result.Id);
        Assert.Equal("Hello", result.Name);
    }

    [Fact]
    public void Deserialize_WithReadOnlyMemory_Should_ReturnObject()
    {
        // Arrange
        var json = "{\"id\":100,\"name\":\"World\"}";
        ReadOnlyMemory<byte> bytes = System.Text.Encoding.UTF8.GetBytes(json);

        // Act
        var result = _serializer.Deserialize<TestMessage>(bytes);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(100, result.Id);
        Assert.Equal("World", result.Name);
    }

    [Fact]
    public void SerializeToString_Should_ReturnJsonString()
    {
        // Arrange
        var obj = new TestMessage { Id = 5, Name = "JsonTest" };

        // Act
        var result = _serializer.SerializeToString(obj);

        // Assert
        Assert.Contains("\"id\":5", result);
        Assert.Contains("\"name\":\"JsonTest\"", result);
    }

    [Fact]
    public void DeserializeFromString_Should_ReturnObject()
    {
        // Arrange
        var json = "{\"id\":999,\"name\":\"StringTest\"}";

        // Act
        var result = _serializer.DeserializeFromString<TestMessage>(json);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(999, result.Id);
        Assert.Equal("StringTest", result.Name);
    }

    [Fact]
    public void Deserialize_WithInvalidJson_Should_ThrowOrReturnNull()
    {
        // Arrange - empty bytes cause JsonException
        byte[] bytes = [];

        // Act & Assert
        // JsonSerializer.Deserialize with empty bytes throws JsonException
        Assert.Throws<System.Text.Json.JsonException>(() => _serializer.Deserialize<TestMessage>(bytes));
    }

    [Fact]
    public void Serialize_WithNullObject_Should_ReturnValidJson()
    {
        // Arrange
        TestMessage obj = new() { Id = 0, Name = null };

        // Act
        var result = _serializer.Serialize(obj);

        // Assert
        Assert.NotNull(result);
    }

    [Fact]
    public void Default_Should_BeSingleton()
    {
        // Act
        var default1 = SystemTextJsonSerializer.Default;
        var default2 = SystemTextJsonSerializer.Default;

        // Assert
        Assert.Same(default1, default2);
    }

    private class TestMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
