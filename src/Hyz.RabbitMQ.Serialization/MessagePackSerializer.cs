using MessagePack.Resolvers;

namespace Hyz.RabbitMQ.Serialization;

/// <summary>
/// MessagePack 序列化器实现
/// </summary>
public class MessagePackSerializer : IMessageSerializer
{
    /// <summary>
    /// 默认实例
    /// </summary>
    public static MessagePackSerializer Default { get; } = new();

    /// <inheritdoc />
    public string ContentType => "application/x-msgpack";

    /// <inheritdoc />
    public byte[] Serialize<T>(T obj) where T : class
    {
        return global::MessagePack.MessagePackSerializer.Serialize(
            obj, TypelessContractlessStandardResolver.Options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> data) where T : class
    {
        return global::MessagePack.MessagePackSerializer.Deserialize<T>(
            data, TypelessContractlessStandardResolver.Options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(byte[] data) where T : class
    {
        return global::MessagePack.MessagePackSerializer.Deserialize<T>(
            data, TypelessContractlessStandardResolver.Options);
    }

    /// <inheritdoc />
    public string SerializeToString<T>(T obj) where T : class
    {
        return Convert.ToBase64String(Serialize(obj));
    }

    /// <inheritdoc />
    public T? DeserializeFromString<T>(string data) where T : class
    {
        var bytes = Convert.FromBase64String(data);
        return Deserialize<T>(bytes);
    }
}
