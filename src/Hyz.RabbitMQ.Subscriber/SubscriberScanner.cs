using Hyz.RabbitMQ.Abstractions;
using Hyz.RabbitMQ.Abstractions.Attributes;
using System.Reflection;

namespace Hyz.RabbitMQ.Subscriber;

/// <summary>
/// 订阅扫描器 - 扫描程序集中的 RabbitMqConsumer 特性标记类
/// </summary>
public static class SubscriberScanner
{
    /// <summary>
    /// 扫描程序集中的所有订阅者类型
    /// </summary>
    /// <param name="assembly">要扫描的程序集</param>
    /// <returns>订阅注册信息列表</returns>
    public static IReadOnlyList<SubscriberRegistration> Scan(Assembly assembly)
    {
        var registrations = new List<SubscriberRegistration>();

        try
        {
            foreach (var type in assembly.GetTypes())
            {
                ProcessType(type, registrations);
            }
        }
        catch (ReflectionTypeLoadException ex)
        {
            // 处理部分类型无法加载的情况（如缺少依赖的程序集）
            foreach (var type in ex.Types.Where(t => t != null))
            {
                ProcessType(type!, registrations);
            }
        }

        return registrations;
    }

    /// <summary>
    /// 扫描程序集中特定类型的订阅者
    /// </summary>
    /// <param name="assembly">要扫描的程序集</param>
    /// <param name="baseType">基类型，只扫描继承自此类型的类</param>
    /// <returns>订阅注册信息列表</returns>
    public static IReadOnlyList<SubscriberRegistration> Scan(Assembly assembly, Type baseType)
    {
        var registrations = new List<SubscriberRegistration>();

        try
        {
            foreach (var type in assembly.GetTypes())
            {
                if (!baseType.IsAssignableFrom(type))
                    continue;
                ProcessType(type, registrations);
            }
        }
        catch (ReflectionTypeLoadException ex)
        {
            foreach (var type in ex.Types.Where(t => t != null))
            {
                if (!baseType.IsAssignableFrom(type!))
                    continue;
                ProcessType(type!, registrations);
            }
        }

        return registrations;
    }

    /// <summary>
    /// 处理单个类型，创建订阅注册信息
    /// </summary>
    private static void ProcessType(Type type, List<SubscriberRegistration> registrations)
    {
        var attr = type.GetCustomAttribute<RabbitMqConsumerAttribute>();
        if (attr == null)
            return;

        registrations.Add(CreateRegistration(type, attr));
    }

    /// <summary>
    /// 根据类型和特性创建订阅注册信息
    /// </summary>
    private static SubscriberRegistration CreateRegistration(Type type, RabbitMqConsumerAttribute attr)
    {
        return new SubscriberRegistration
        {
            Name = type.FullName ?? type.Name,
            QueueName = attr.Queue,
            ExchangeName = attr.Exchange ?? string.Empty,
            RoutingKey = attr.RoutingKey ?? string.Empty,
            ConnectionName = attr.ConnectionName ?? "Default",
            ConsumerType = type,
            AutoAck = attr.AutoAck,
            PrefetchCount = attr.PrefetchCount,
            Durable = attr.Durable,
            MaxRetryCount = attr.MaxRetryCount,
            DeadLetterExchange = attr.DeadLetterExchange,
            DeadLetterRoutingKey = attr.DeadLetterRoutingKey,
            TagPrefix = attr.TagPrefix
        };
    }
}
