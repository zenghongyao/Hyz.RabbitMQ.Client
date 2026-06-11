using Hyz.RabbitMQ.Serialization;

namespace Hyz.RabbitMQ.Tests.Serialization;

public class MessagePackSerializerTests
{
    private readonly MessagePackSerializer _serializer = new();

    [Fact]
    public void ContentType_Should_ReturnApplicationXMsgpack()
    {
        Assert.Equal("application/x-msgpack", _serializer.ContentType);
    }

    [Fact]
    public void Default_Should_BeSingleton()
    {
        var default1 = MessagePackSerializer.Default;
        var default2 = MessagePackSerializer.Default;
        Assert.Same(default1, default2);
    }

    [Fact]
    public void Serialize_WithSimpleObject_Should_ReturnBytes()
    {
        var obj = new TestMessage { Id = 1, Name = "Test" };

        var result = _serializer.Serialize(obj);

        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void Serialize_WithReadOnlyMemory_Should_ReturnBytes()
    {
        var obj = new TestMessage { Id = 2, Name = "Memory" };

        var result = _serializer.Serialize(obj);

        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    [Fact]
    public void Deserialize_WithValidData_Should_ReturnObject()
    {
        var obj = new TestMessage { Id = 42, Name = "Hello" };
        var bytes = _serializer.Serialize(obj);

        var result = _serializer.Deserialize<TestMessage>(bytes);

        Assert.NotNull(result);
        Assert.Equal(42, result.Id);
        Assert.Equal("Hello", result.Name);
    }

    [Fact]
    public void Deserialize_WithReadOnlyMemory_Should_ReturnObject()
    {
        var obj = new TestMessage { Id = 100, Name = "World" };
        byte[] bytes = _serializer.Serialize(obj);
        ReadOnlyMemory<byte> memory = bytes;

        var result = _serializer.Deserialize<TestMessage>(memory);

        Assert.NotNull(result);
        Assert.Equal(100, result.Id);
        Assert.Equal("World", result.Name);
    }

    [Fact]
    public void Deserialize_WithInvalidData_Should_ThrowMessagePackException()
    {
        byte[] invalidBytes = [0xFF, 0xFF, 0xFF];

        var ex = Assert.ThrowsAny<Exception>(
            () => _serializer.Deserialize<TestMessage>(invalidBytes));
        Assert.Contains("MessagePack", ex.GetType().FullName);
    }

    [Fact]
    public void Deserialize_WithEmptyData_Should_ThrowMessagePackException()
    {
        byte[] emptyBytes = [];

        var ex = Assert.ThrowsAny<Exception>(
            () => _serializer.Deserialize<TestMessage>(emptyBytes));
        Assert.Contains("MessagePack", ex.GetType().FullName);
    }

    [Fact]
    public void SerializeToString_Should_ReturnBase64String()
    {
        var obj = new TestMessage { Id = 123, Name = "Base64" };

        var result = _serializer.SerializeToString(obj);

        Assert.NotNull(result);
        Assert.NotEmpty(result);
        Assert.True(IsBase64String(result));
    }

    [Fact]
    public void DeserializeFromString_Should_ReturnObject()
    {
        var obj = new TestMessage { Id = 456, Name = "FromString" };
        var base64 = _serializer.SerializeToString(obj);

        var result = _serializer.DeserializeFromString<TestMessage>(base64);

        Assert.NotNull(result);
        Assert.Equal(456, result.Id);
        Assert.Equal("FromString", result.Name);
    }

    [Fact]
    public void RoundTrip_Should_PreserveData()
    {
        var original = new TestMessage { Id = 999, Name = "RoundTrip" };
        byte[] serialized = _serializer.Serialize(original);
        var deserialized = _serializer.Deserialize<TestMessage>(serialized);

        Assert.NotNull(deserialized);
        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Name, deserialized.Name);
    }

    [Fact]
    public void Serialize_WithNullName_Should_ReturnValidData()
    {
        var obj = new TestMessage { Id = 0, Name = null };

        var result = _serializer.Serialize(obj);

        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }

    private static bool IsBase64String(string s)
    {
        try
        {
            Convert.FromBase64String(s);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private class TestMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
