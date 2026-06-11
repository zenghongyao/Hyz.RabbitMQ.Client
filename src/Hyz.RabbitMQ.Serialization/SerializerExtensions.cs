using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Serialization;

/// <summary>
/// MessageBody 序列化扩展方法
/// </summary>
public static class MessageBodySerializerExtensions
{
    /// <summary>
    /// 从对象序列化为 MessageBody
    /// </summary>
    public static MessageBody ToMessageBody<T>(this T obj, IMessageSerializer? serializer = null) where T : class
    {
        serializer ??= SystemTextJsonSerializer.Default;
        return new MessageBody(serializer.Serialize(obj));
    }

    /// <summary>
    /// 从 MessageBody 反序列化为对象
    /// </summary>
    public static T? FromMessageBody<T>(this ReadOnlyMemory<byte> body, IMessageSerializer? serializer = null) where T : class
    {
        serializer ??= SystemTextJsonSerializer.Default;
        return serializer.Deserialize<T>(body);
    }

    /// <summary>
    /// 从 MessageBody 反序列化为对象
    /// </summary>
    public static T? FromMessageBody<T>(this MessageBody body, IMessageSerializer? serializer = null) where T : class
    {
        return body.Bytes.FromMessageBody<T>(serializer);
    }

    /// <summary>
    /// 从字符串创建 MessageBody
    /// </summary>
    public static MessageBody ToMessageBodyFromString(string content, System.Text.Encoding? encoding = null)
    {
        encoding ??= System.Text.Encoding.UTF8;
        return new MessageBody(encoding.GetBytes(content).AsMemory());
    }

    /// <summary>
    /// 将 MessageBody 转换为字符串
    /// </summary>
    public static string ToStringContent(this MessageBody body, System.Text.Encoding? encoding = null)
    {
        encoding ??= System.Text.Encoding.UTF8;
#if NETSTANDARD2_0
        return encoding.GetString(body.Bytes.ToArray());
#else
        return encoding.GetString(body.Bytes.Span);
#endif
    }
}

/// <summary>
/// 序列化器扩展
/// </summary>
public static class SerializerExtensions
{
    /// <summary>
    /// 创建 JSON 序列化器
    /// </summary>
    public static IMessageSerializer CreateJsonSerializer(this IMessageSerializer serializer)
    {
        return serializer;
    }

    /// <summary>
    /// 获取序列化器的 Content-Type
    /// </summary>
    public static string GetContentType(this IMessageSerializer serializer)
    {
        return serializer.ContentType;
    }
}
