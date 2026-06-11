using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Tests.Abstractions;

public class MessageBodyTests
{
    [Fact]
    public void DefaultConstructor_Should_CreateEmptyBody()
    {
        // Arrange & Act
        var body = new MessageBody();

        // Assert
        Assert.True(body.Bytes.IsEmpty);
    }

    [Fact]
    public void Constructor_WithBytes_Should_StoreBytes()
    {
        // Arrange
        byte[] data = [1, 2, 3, 4, 5];

        // Act
        var body = new MessageBody(data.AsMemory());

        // Assert
        Assert.Equal(5, body.Bytes.Length);
    }

    [Fact]
    public void FromString_Should_ConvertToUtf8Bytes()
    {
        // Arrange
        string content = "Hello, RabbitMQ!";

        // Act
        var body = MessageBody.FromString(content);

        // Assert
        var decoded = System.Text.Encoding.UTF8.GetString(body.Bytes.Span);
        Assert.Equal(content, decoded);
    }

    [Fact]
    public void FromString_WithCustomEncoding_Should_UseProvidedEncoding()
    {
        // Arrange
        string content = "你好";
        var encoding = System.Text.Encoding.UTF8;

        // Act
        var body = MessageBody.FromString(content, encoding);

        // Assert
        var decoded = encoding.GetString(body.Bytes.Span);
        Assert.Equal(content, decoded);
    }

    [Fact]
    public void FromString_WithNullEncoding_Should_DefaultToUtf8()
    {
        // Arrange
        string content = "Test Content";

        // Act
        var body = MessageBody.FromString(content, null);

        // Assert
        var decoded = System.Text.Encoding.UTF8.GetString(body.Bytes.Span);
        Assert.Equal(content, decoded);
    }

    [Fact]
    public void FromBytes_Should_CreateFromByteArray()
    {
        // Arrange
        byte[] data = [1, 2, 3];

        // Act
        var body = MessageBody.FromBytes(data);

        // Assert
        Assert.Equal(3, body.Bytes.Length);
    }

    [Fact]
    public void ImplicitConversion_FromByteArray_Should_Work()
    {
        // Arrange
        byte[] data = [1, 2, 3, 4, 5];

        // Act
        MessageBody body = data;

        // Assert
        Assert.Equal(5, body.Bytes.Length);
    }

    [Fact]
    public void ImplicitConversion_FromReadOnlyMemory_Should_Work()
    {
        // Arrange
        ReadOnlyMemory<byte> data = new byte[] { 1, 2, 3 };

        // Act
        MessageBody body = data;

        // Assert
        Assert.Equal(3, body.Bytes.Length);
    }
}
