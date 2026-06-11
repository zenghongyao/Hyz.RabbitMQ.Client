using System.Collections.Concurrent;
using Hyz.RabbitMQ.Abstractions;

namespace Hyz.RabbitMQ.Core;

/// <summary>
/// 连接管理器，负责集中管理多个 RabbitMQ 连接提供者（IConnectionProvider）。
/// 支持按名称注册和获取连接，并维护一个默认连接实例供便捷访问。
/// </summary>
public class ConnectionManager : IConnectionManager
{
    /// <summary>
    /// 线程安全的连接提供者字典，以连接名称为键存储对应的 IConnectionProvider 实例。
    /// </summary>
    private readonly ConcurrentDictionary<string, IConnectionProvider> _providers = new();

    /// <summary>
    /// 默认连接提供者，首次注册时自动设为默认。
    /// </summary>
    private IConnectionProvider? _defaultProvider;

    /// <summary>
    /// 用于保护 _defaultProvider 赋值操作的锁对象。
    /// </summary>
    private readonly object _lock = new();

    /// <summary>
    /// 获取默认连接提供者（第一个注册或显式标记为默认的连接）。
    /// 如果尚未注册任何连接，抛出 <see cref="InvalidOperationException"/>。
    /// </summary>
    /// <exception cref="InvalidOperationException">当没有任何连接被注册时抛出。</exception>
    public IConnectionProvider Default
    {
        get
        {
            if (_defaultProvider == null)
            {
                throw new InvalidOperationException("No default connection registered.");
            }
            return _defaultProvider;
        }
    }

    /// <summary>
    /// 注册一个连接提供者到管理器中。可选指定是否为默认连接。
    /// 首次注册、或 <paramref name="isDefault"/> 为 true 时，该连接成为默认连接。
    /// 同一名称的重复注册将被忽略（幂等操作）。
    /// 如果注册的连接名称为 "Default"，将无条件设为默认连接。
    /// </summary>
    /// <param name="provider">要注册的连接提供者实例。</param>
    /// <param name="isDefault">是否将此连接设为默认连接，默认为 false（首次注册自动升为默认）。</param>
    public void Register(IConnectionProvider provider, bool isDefault = false)
    {
        var name = provider.Name;

        // 原子性注册：ConcurrentDictionary.GetOrAdd 内部保证线程安全，
        // 同一键重复调用不会覆盖已有值。
        _providers.GetOrAdd(name, provider);

        // 如果是 "Default" 连接名称，或者明确指定 isDefault（且当前没有默认），则设为默认
        if (name == ConnectionConstants.DefaultConnectionName)
        {
            lock (_lock)
            {
                _defaultProvider = provider;
            }
        }
        else if (isDefault || _providers.Count == 1)
        {
            lock (_lock)
            {
                _defaultProvider ??= provider;
            }
        }
    }

    /// <summary>
    /// 根据连接名称获取对应的连接提供者。
    /// </summary>
    /// <param name="name">连接名称，默认为 "Default"。</param>
    /// <returns>对应的 IConnectionProvider 实例。</returns>
    /// <exception cref="KeyNotFoundException">当指定名称的连接不存在时抛出。</exception>
    public IConnectionProvider GetProvider(string name = ConnectionConstants.DefaultConnectionName)
    {
        if (_providers.TryGetValue(name, out var provider))
        {
            return provider;
        }

        throw new KeyNotFoundException($"Connection '{name}' not found.");
    }

    /// <summary>
    /// 异步获取指定连接的底层 RabbitMQ IConnection 对象。
    /// 如果连接尚未建立，则触发自动创建（内部通过 IConnectionProvider）。
    /// </summary>
    /// <param name="name">连接名称，默认为 "Default"。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>可用的 RabbitMQ IConnection 实例。</returns>
    public async Task<global::RabbitMQ.Client.IConnection> GetConnectionAsync(
        string name = ConnectionConstants.DefaultConnectionName,
        CancellationToken cancellationToken = default)
    {
        var provider = GetProvider(name);
        return await provider.GetConnectionAsync(cancellationToken);
    }

    /// <summary>
    /// 获取所有已注册连接的名称列表。
    /// </summary>
    /// <returns>只读字符串集合，包含所有连接名称。</returns>
    public IReadOnlyCollection<string> GetAllConnectionNames()
    {
        return _providers.Keys.ToList().AsReadOnly();
    }

    /// <summary>
    /// 检查指定名称的连接是否已注册。
    /// </summary>
    /// <param name="name">要检查的连接名称。</param>
    /// <returns>已注册返回 true，否则返回 false。</returns>
    public bool Contains(string name)
    {
        return _providers.ContainsKey(name);
    }
}
