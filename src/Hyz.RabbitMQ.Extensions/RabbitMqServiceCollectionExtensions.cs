using System.Collections.Concurrent;
using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;

using Core = Hyz.RabbitMQ.Core;

namespace Hyz.RabbitMQ.Extensions;

/// <summary>
/// RabbitMQ DI 扩展，提供 AddRabbitMq 系列方法注册 RabbitMQ 连接及相关服务。
/// 支持多个连接按名称隔离注册，通过 Keyed Services 实现按名称解析。
/// </summary>
public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    /// 注册标记接口，用于在 DI 容器中标识 RabbitMQ 连接管理器集合
    /// </summary>
    private interface IConnectionManagerRegistry { }

    /// <summary>
    /// 连接管理器注册表，用于在同一个 IServiceCollection 中追踪和管理多个连接
    /// </summary>
    private sealed class ConnectionManagerRegistry : IConnectionManagerRegistry
    {
        public Core.ConnectionManager Manager { get; } = new();
        public ConcurrentDictionary<string, bool> RegisteredConnections { get; } = new();
    }

    /// <summary>
    /// 获取或创建连接管理器注册表
    /// </summary>
    private static ConnectionManagerRegistry GetOrCreateRegistry(IServiceCollection services)
    {
        // 尝试获取现有注册表
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IConnectionManagerRegistry));
        if (descriptor?.ImplementationInstance is ConnectionManagerRegistry existing)
        {
            return existing;
        }

        // 创建新注册表
        var registry = new ConnectionManagerRegistry();
        services.AddSingleton<IConnectionManagerRegistry>(registry);
        services.AddSingleton<IConnectionManager>(registry.Manager);
        return registry;
    }

    /// <summary>
    /// 添加 RabbitMQ 连接（使用默认名称 "Default"）。
    /// </summary>
    public static IServiceCollection AddRabbitMq(
        this IServiceCollection services,
        Action<RabbitMqConnectionOptions> configure)
    {
        return AddRabbitMq(services, ConnectionConstants.DefaultConnectionName, configure);
    }

    /// <summary>
    /// 添加 RabbitMQ 连接（指定连接名称）。
    /// 多次调用 AddRabbitMq 注册多个连接时，第一个注册的连接成为默认连接。
    /// </summary>
    public static IServiceCollection AddRabbitMq(
        this IServiceCollection services,
        string connectionName,
        Action<RabbitMqConnectionOptions> configure)
    {
        var options = new RabbitMqConnectionOptions { Name = connectionName };
        configure(options);
        options.Name = connectionName;

        // 获取或创建连接管理器注册表
        var registry = GetOrCreateRegistry(services);

        // 为当前连接创建专属的 ConnectionProvider
        var provider = new Core.RabbitMqConnectionProvider(options);

        // 注册到 ConnectionManager
        bool isFirst = !registry.RegisteredConnections.ContainsKey(connectionName);
        registry.RegisteredConnections.TryAdd(connectionName, true);
        registry.Manager.Register(provider, isDefault: isFirst);

        // 注册 IConnectionProvider（支持按名称 Keyed 解析和默认解析）
        services.AddKeyedSingleton<IConnectionProvider, Core.RabbitMqConnectionProvider>(connectionName, (sp, key) => provider);

        // 仅第一个注册为默认连接
        if (isFirst)
        {
            services.AddSingleton<IConnectionProvider>(provider);
        }

        // 注册连接选项
        // 使用 Keyed Service 注册，支持按名称解析
        // 同时注册一个工厂方法用于默认解析，确保返回同一实例
        services.AddKeyedSingleton<RabbitMqConnectionOptions>(connectionName, options);
        if (isFirst)
        {
            // 只有第一个连接注册的选项作为默认选项
            services.AddSingleton(options);
        }

        // 注册默认的 ConsumerService（如果尚未注册）
        services.AddScoped<IConsumerService>(sp =>
        {
            var mgr = sp.GetRequiredService<IConnectionManager>();
            return new Core.ConsumerService(mgr.Default);
        });

        // 注册默认的 PublisherService（如果尚未注册）
        services.AddScoped<IPublisherService>(sp =>
        {
            var mgr = sp.GetRequiredService<IConnectionManager>();
            return new Core.PublisherService(mgr.Default);
        });

        // 注册多连接服务（如果尚未注册）
        services.AddScoped<IMultiConnectionPublisherService, Core.MultiConnectionPublisherService>();
        services.AddScoped<IMultiConnectionConsumerService, Core.MultiConnectionConsumerService>();

        return services;
    }

    /// <summary>
    /// 添加指定连接的 PublisherService
    /// </summary>
    /// <param name="connectionName">连接名称</param>
    /// <exception cref="ArgumentException">当连接名称对应的连接不存在时抛出</exception>
    public static IServiceCollection AddRabbitMqPublisher(
        this IServiceCollection services,
        string connectionName)
    {
        #if !NETSTANDARD2_0
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
#else
        if (string.IsNullOrWhiteSpace(connectionName))
            throw new ArgumentException("Connection name cannot be null or whitespace.", nameof(connectionName));
#endif

        services.AddKeyedScoped<IPublisherService, Core.PublisherService>(connectionName, (sp, key) =>
        {
            var mgr = sp.GetRequiredService<IConnectionManager>();
            if (!mgr.Contains(connectionName))
            {
                throw new ArgumentException(
                    $"Connection '{connectionName}' is not registered. Please call AddRabbitMq first.",
                    nameof(connectionName));
            }
            return new Core.PublisherService(mgr.GetProvider(connectionName));
        });
        return services;
    }

    /// <summary>
    /// 添加指定连接的 ConsumerService
    /// </summary>
    /// <param name="connectionName">连接名称</param>
    /// <exception cref="ArgumentException">当连接名称对应的连接不存在时抛出</exception>
    public static IServiceCollection AddRabbitMqConsumer(
        this IServiceCollection services,
        string connectionName)
    {
#if !NETSTANDARD2_0
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionName);
#else
        if (string.IsNullOrWhiteSpace(connectionName))
            throw new ArgumentException("Connection name cannot be null or whitespace.", nameof(connectionName));
#endif

        services.AddKeyedScoped<IConsumerService, Core.ConsumerService>(connectionName, (sp, key) =>
        {
            var mgr = sp.GetRequiredService<IConnectionManager>();
            if (!mgr.Contains(connectionName))
            {
                throw new ArgumentException(
                    $"Connection '{connectionName}' is not registered. Please call AddRabbitMq first.",
                    nameof(connectionName));
            }
            return new Core.ConsumerService(mgr.GetProvider(connectionName));
        });
        return services;
    }

    /// <summary>
    /// 添加 RabbitMQ 连接（使用已有的 RabbitMqConnectionOptions 配置对象）。
    /// </summary>
    public static IServiceCollection AddRabbitMq(
        this IServiceCollection services,
        RabbitMqConnectionOptions options)
    {
        return AddRabbitMq(services, options.GetConnectionName(), opts =>
        {
            opts.HostName = options.HostName;
            opts.Port = options.Port;
            opts.UserName = options.UserName;
            opts.Password = options.Password;
            opts.VirtualHost = options.VirtualHost;
            opts.Heartbeat = options.Heartbeat;
            opts.AutoReconnect = options.AutoReconnect;
            opts.MaxRetryCount = options.MaxRetryCount;
            opts.RetryDelayMs = options.RetryDelayMs;
            opts.EnableTls = options.EnableTls;
            opts.TlsOptions = options.TlsOptions;
        });
    }
}
