namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 消息体
/// </summary>
public readonly struct MessageBody
{
    /// <summary>
    /// 消息内容
    /// </summary>
    public ReadOnlyMemory<byte> Bytes { get; }

    /// <summary>
    /// 创建消息体
    /// </summary>
    public MessageBody(ReadOnlyMemory<byte> bytes)
    {
        Bytes = bytes;
    }

    /// <summary>
    /// 从字符串创建消息体
    /// </summary>
    public static MessageBody FromString(string content, System.Text.Encoding? encoding = null)
    {
        encoding ??= System.Text.Encoding.UTF8;
        return new MessageBody(encoding.GetBytes(content).AsMemory());
    }

    /// <summary>
    /// 从字节数组创建消息体
    /// </summary>
    public static MessageBody FromBytes(byte[] bytes)
    {
        return new MessageBody(bytes.AsMemory());
    }

    /// <summary>
    /// 隐式转换
    /// </summary>
    public static implicit operator MessageBody(ReadOnlyMemory<byte> bytes) => new(bytes);

    /// <summary>
    /// 隐式转换
    /// </summary>
    public static implicit operator MessageBody(byte[] bytes) => new(bytes.AsMemory());
}
