using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Hyz.RabbitMQ.Serialization;

/// <summary>
/// System.Text.Json 序列化器实现
/// </summary>
public class SystemTextJsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>
    /// 默认实例
    /// </summary>
    public static SystemTextJsonSerializer Default { get; } = new();

    /// <inheritdoc />
    public string ContentType => "application/json";

    /// <summary>
    /// 创建序列化器
    /// </summary>
    public SystemTextJsonSerializer()
    {
        _options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = true
        };
    }

    /// <summary>
    /// 使用自定义选项创建序列化器
    /// </summary>
    public SystemTextJsonSerializer(JsonSerializerOptions options)
    {
        _options = options;
    }

    /// <inheritdoc />
    public byte[] Serialize<T>(T obj) where T : class
    {
        return JsonSerializer.SerializeToUtf8Bytes(obj, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data.Span, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(byte[] data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }

    /// <inheritdoc />
    public string SerializeToString<T>(T obj) where T : class
    {
        return JsonSerializer.Serialize(obj, _options);
    }

    /// <inheritdoc />
    public T? DeserializeFromString<T>(string data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }
}

/// <summary>
/// 带 UTF8 编码优化的序列化器
/// </summary>
public class Utf8JsonSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <inheritdoc />
    public string ContentType => "application/json; charset=utf-8";

    /// <summary>
    /// 创建序列化器
    /// </summary>
    public Utf8JsonSerializer()
    {
        _options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            PropertyNameCaseInsensitive = true
        };
    }

    /// <inheritdoc />
    public byte[] Serialize<T>(T obj) where T : class
    {
        using var stream = new MemoryStream();
        JsonSerializer.Serialize(stream, obj, _options);
        return stream.ToArray();
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data.Span, _options);
    }

    /// <inheritdoc />
    public T? Deserialize<T>(byte[] data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }

    /// <inheritdoc />
    public string SerializeToString<T>(T obj) where T : class
    {
        return JsonSerializer.Serialize(obj, _options);
    }

    /// <inheritdoc />
    public T? DeserializeFromString<T>(string data) where T : class
    {
        return JsonSerializer.Deserialize<T>(data, _options);
    }
}
