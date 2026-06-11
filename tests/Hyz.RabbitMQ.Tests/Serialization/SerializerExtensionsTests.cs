using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Serialization;

namespace Hyz.RabbitMQ.Tests.Serialization;

public class SerializerExtensionsTests
{
    [Fact]
    public void ToMessageBody_WithObject_Should_ConvertToMessageBody()
    {
        // Arrange
        var obj = new TestMessage { Id = 1, Name = "Test" };

        // Act
        var result = obj.ToMessageBody();

        // Assert
        Assert.False(result.Bytes.IsEmpty);
    }

    [Fact]
    public void ToMessageBody_WithCustomSerializer_Should_UseProvidedSerializer()
    {
        // Arrange
        var obj = new TestMessage { Id = 2, Name = "Custom" };
        var serializer = new SystemTextJsonSerializer();

        // Act
        var result = obj.ToMessageBody(serializer);

        // Assert
        Assert.False(result.Bytes.IsEmpty);
    }

    [Fact]
    public void FromMessageBody_WithReadOnlyMemory_Should_DeserializeToObject()
    {
        // Arrange
        var obj = new TestMessage { Id = 3, Name = "FromBody" };
        var serializer = new SystemTextJsonSerializer();
        var body = obj.ToMessageBody(serializer);

        // Act
        var result = body.Bytes.FromMessageBody<TestMessage>(serializer);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(obj.Id, result.Id);
        Assert.Equal(obj.Name, result.Name);
    }

    [Fact]
    public void FromMessageBody_WithMessageBody_Should_DeserializeToObject()
    {
        // Arrange
        var obj = new TestMessage { Id = 4, Name = "FromBody2" };
        var body = obj.ToMessageBody();

        // Act
        var result = body.FromMessageBody<TestMessage>();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(obj.Id, result.Id);
        Assert.Equal(obj.Name, result.Name);
    }

    [Fact]
    public void ToMessageBodyFromString_Should_ConvertToMessageBody()
    {
        // Arrange
        var content = "Hello, World!";

        // Act
        var result = MessageBodySerializerExtensions.ToMessageBodyFromString(content);

        // Assert
        Assert.False(result.Bytes.IsEmpty);
    }

    [Fact]
    public void ToMessageBodyFromString_WithCustomEncoding_Should_UseProvidedEncoding()
    {
        // Arrange
        var content = "你好";
        var encoding = System.Text.Encoding.UTF8;

        // Act
        var result = MessageBodySerializerExtensions.ToMessageBodyFromString(content, encoding);

        // Assert
        Assert.Equal(6, result.Bytes.Length);
    }

    [Fact]
    public void ToStringContent_Should_ConvertToString()
    {
        // Arrange
        var content = "Test Content";
        var body = MessageBodySerializerExtensions.ToMessageBodyFromString(content);

        // Act
        var result = MessageBodySerializerExtensions.ToStringContent(body);

        // Assert
        Assert.Equal(content, result);
    }

    [Fact]
    public void ToStringContent_WithCustomEncoding_Should_UseProvidedEncoding()
    {
        // Arrange
        var content = "Hello";
        var body = MessageBody.FromString(content, System.Text.Encoding.UTF8);

        // Act
        var result = body.ToStringContent(System.Text.Encoding.UTF8);

        // Assert
        Assert.Equal(content, result);
    }

    [Fact]
    public void GetContentType_Should_ReturnContentType()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer();

        // Act
        var result = serializer.GetContentType();

        // Assert
        Assert.Equal("application/json", result);
    }

    [Fact]
    public void CreateJsonSerializer_Should_ReturnSameSerializer()
    {
        // Arrange
        var serializer = new SystemTextJsonSerializer();

        // Act
        var result = serializer.CreateJsonSerializer();

        // Assert
        Assert.Same(serializer, result);
    }

    private class TestMessage
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }
}
