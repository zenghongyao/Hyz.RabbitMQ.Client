using Hyz.RabbitMQ.Serialization;

namespace Hyz.RabbitMQ.Tests.Serialization;

public class Utf8JsonSerializerTests
{
    private readonly Utf8JsonSerializer _serializer = new();

    [Fact]
    public void ContentType_Should_ReturnUtf8JsonContentType()
    {
        Assert.Equal("application/json; charset=utf-8", _serializer.ContentType);
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
    public void RoundTrip_Should_PreserveData()
    {
        // Arrange
        var original = new TestMessage { Id = 12345, Name = "RoundTrip Test" };

        // Act
        var serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<TestMessage>(serialized);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Name, deserialized.Name);
    }

    private class TestMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
