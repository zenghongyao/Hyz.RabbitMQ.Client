namespace Hyz.RabbitMQ.Serialization;

/// <summary>
/// 消息序列化器接口
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// 序列化对象为字节数组
    /// </summary>
    byte[] Serialize<T>(T obj) where T : class;

    /// <summary>
    /// 反序列化字节数组为对象
    /// </summary>
    T? Deserialize<T>(ReadOnlyMemory<byte> data) where T : class;

    /// <summary>
    /// 反序列化字节数组为对象
    /// </summary>
    T? Deserialize<T>(byte[] data) where T : class;

    /// <summary>
    /// 将对象序列化为字符串
    /// </summary>
    string SerializeToString<T>(T obj) where T : class;

    /// <summary>
    /// 从字符串反序列化对象
    /// </summary>
    T? DeserializeFromString<T>(string data) where T : class;

    /// <summary>
    /// 获取内容类型标识
    /// </summary>
    string ContentType { get; }
}
