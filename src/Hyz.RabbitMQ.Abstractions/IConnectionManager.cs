using RabbitMQ.Client;

namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 连接管理器常量
/// </summary>
public static class ConnectionConstants
{
    /// <summary>
    /// 默认连接名称
    /// </summary>
    public const string DefaultConnectionName = "Default";
}

/// <summary>
/// 连接管理器接口 - 管理多个连接
/// </summary>
public interface IConnectionManager
{
    /// <summary>
    /// 获取指定名称的连接提供者
    /// </summary>
    IConnectionProvider GetProvider(string name = ConnectionConstants.DefaultConnectionName);

    /// <summary>
    /// 异步获取指定名称的连接
    /// </summary>
    Task<IConnection> GetConnectionAsync(
        string name = ConnectionConstants.DefaultConnectionName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取所有已注册的连接名称
    /// </summary>
    IReadOnlyCollection<string> GetAllConnectionNames();

    /// <summary>
    /// 检查连接是否存在
    /// </summary>
    bool Contains(string name);

    /// <summary>
    /// 获取默认连接提供者
    /// </summary>
    IConnectionProvider Default { get; }

    /// <summary>
    /// 注册连接
    /// </summary>
    void Register(IConnectionProvider provider, bool isDefault = false);
}

/// <summary>
/// 连接注册信息
/// </summary>
public record ConnectionRegistration
{
    /// <summary>
    /// 连接名称
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// 连接配置
    /// </summary>
    public required RabbitMqConnectionOptions Options { get; init; }

    /// <summary>
    /// 是否为默认连接
    /// </summary>
    public bool IsDefault { get; init; }
}
