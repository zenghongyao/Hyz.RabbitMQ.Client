using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Hyz.RabbitMQ.Generator;

/// <summary>
/// RabbitMQ 消费者源代码生成器。
/// 实现 <see cref="ISourceGenerator"/> 接口，在编译时分析代码中的 RabbitMQ 特性标记（Attribute），
/// 自动生成服务注册扩展、交换机/队列声明基础设施和消息处理扩展方法。
/// 支持的特性标记包括：
/// <list type="bullet">
///   <item>[RabbitMqConsumer] — 标记消费类</item>
///   <item>[RabbitMqExchange] — 声明交换机</item>
///   <item>[RabbitMqQueue] — 声明队列</item>
///   <item>[RabbitMqBinding] — 声明绑定关系</item>
///   <item>[RabbitMqSubscribe] — 标记订阅方法（单条消息）</item>
///   <item>[RabbitMqBatchSubscribe] — 标记批量订阅方法</item>
/// </list>
/// </summary>
[Generator]
public class RabbitMqConsumerGenerator : ISourceGenerator
{
    /// <summary>
    /// 初始化阶段注册语法接收器，用于收集包含 RabbitMQ 特性标记的类和成员。
    /// </summary>
    /// <param name="context">生成器初始化上下文。</param>
    public void Initialize(GeneratorInitializationContext context)
    {
        context.RegisterForSyntaxNotifications(() => new ConsumerSyntaxReceiver());
    }

    /// <summary>
    /// 执行阶段：遍历所有被语法接收器收集的消费者类，
    /// 为每个类生成注册扩展类、基础设施声明类和消息处理扩展类。
    /// </summary>
    /// <param name="context">生成器执行上下文，用于添加生成的源文件。</param>
    public void Execute(GeneratorExecutionContext context)
    {
        if (context.SyntaxReceiver is not ConsumerSyntaxReceiver receiver)
            return;

        foreach (var classDecl in receiver.Consumers)
        {
            var namespaceName = GetNamespace(classDecl);
            var className = classDecl.Identifier.Text;
            var fullName = GetFullName(classDecl);

            var consumerInfo = GetConsumerRegistrationInfo(classDecl);
            var methodSubscriptions = GetMethodSubscriptions(classDecl);
            var exchanges = GetExchangeDeclarations(classDecl);
            var queues = GetQueueDeclarations(classDecl);
            var bindings = GetBindingDeclarations(classDecl);

            // 生成消费者注册扩展类（AddXxx / AddXxxConsumer / AddXxxSubscriber）
            if (consumerInfo is not null)
            {
                var registrationSource = GenerateRegistrationSource(
                    namespaceName,
                    className,
                    fullName,
                    consumerInfo);

                context.AddSource($"{className}_Registration.g.cs", SourceText.From(registrationSource, Encoding.UTF8));
            }

            // 生成基础设施声明类（声明交换机/队列/绑定）
            if (exchanges.Count > 0 || queues.Count > 0 || bindings.Count > 0)
            {
                var infraSource = GenerateInfrastructureSource(
                    namespaceName,
                    className,
                    exchanges,
                    queues,
                    bindings);

                context.AddSource($"{className}_Infrastructure.g.cs", SourceText.From(infraSource, Encoding.UTF8));
            }

            // 为每个订阅方法生成独立的处理扩展类
            foreach (var method in methodSubscriptions)
            {
                var handlerSource = GenerateHandlerExtension(
                    namespaceName,
                    className,
                    method);

                context.AddSource($"{className}_{method.MethodName}_Handler.g.cs", SourceText.From(handlerSource, Encoding.UTF8));
            }
        }
    }

    /// <summary>
    /// 从类的 [RabbitMqConsumer] 特性中提取消费者注册信息。
    /// </summary>
    /// <param name="classDecl">消费者类声明语法节点。</param>
    /// <returns>解析得到的 ConsumerRegistrationInfo，若不存在对应特性则返回 null。</returns>
    private static ConsumerRegistrationInfo? GetConsumerRegistrationInfo(ClassDeclarationSyntax classDecl)
    {
        foreach (var attr in GetAttributes(classDecl))
        {
            if (attr.Name.ToString().Contains("RabbitMqConsumer") && attr is AttributeSyntax consumerAttr)
            {
                return new ConsumerRegistrationInfo
                {
                    QueueName = GetAttributeArgumentValue(consumerAttr, "Queue") ?? string.Empty,
                    Exchange = GetAttributeArgumentValue(consumerAttr, "Exchange"),
                    RoutingKey = GetAttributeArgumentValue(consumerAttr, "RoutingKey"),
                    ConnectionName = GetAttributeArgumentValue(consumerAttr, "ConnectionName"),
                    AutoAck = GetBoolAttributeValue(consumerAttr, "AutoAck") ?? false,
                    PrefetchCount = GetUshortAttributeValue(consumerAttr, "PrefetchCount") ?? 10,
                    Durable = GetBoolAttributeValue(consumerAttr, "Durable") ?? true,
                    MaxRetryCount = GetIntAttributeValue(consumerAttr, "MaxRetryCount") ?? 3,
                    DeadLetterExchange = GetAttributeArgumentValue(consumerAttr, "DeadLetterExchange"),
                    DeadLetterRoutingKey = GetAttributeArgumentValue(consumerAttr, "DeadLetterRoutingKey"),
                };
            }
        }

        return null;
    }

    /// <summary>
    /// 从类的 [RabbitMqExchange] 特性列表中提取所有交换机声明信息。
    /// </summary>
    private static List<ExchangeDeclarationInfo> GetExchangeDeclarations(ClassDeclarationSyntax classDecl)
    {
        var result = new List<ExchangeDeclarationInfo>();

        foreach (var attr in GetAttributes(classDecl))
        {
            if (attr.Name.ToString().Contains("RabbitMqExchange") && attr is AttributeSyntax exchangeAttr)
            {
                var typeValue = GetAttributeArgumentValue(exchangeAttr, "Type") ?? "Direct";
                result.Add(new ExchangeDeclarationInfo
                {
                    Name = GetRequiredAttributeArgumentValue(exchangeAttr, "Name"),
                    Type = typeValue,
                    Durable = GetBoolAttributeValue(exchangeAttr, "Durable") ?? true,
                    AutoDelete = GetBoolAttributeValue(exchangeAttr, "AutoDelete") ?? false,
                    Arguments = GetAttributeArgumentValue(exchangeAttr, "Arguments"),
                });
            }
        }

        return result;
    }

    /// <summary>
    /// 从类的 [RabbitMqQueue] 特性列表中提取所有队列声明信息。
    /// </summary>
    private static List<QueueDeclarationInfo> GetQueueDeclarations(ClassDeclarationSyntax classDecl)
    {
        var result = new List<QueueDeclarationInfo>();

        foreach (var attr in GetAttributes(classDecl))
        {
            if (attr.Name.ToString().Contains("RabbitMqQueue") && attr is AttributeSyntax queueAttr)
            {
                result.Add(new QueueDeclarationInfo
                {
                    Name = GetRequiredAttributeArgumentValue(queueAttr, "Name"),
                    Durable = GetBoolAttributeValue(queueAttr, "Durable") ?? true,
                    Exclusive = GetBoolAttributeValue(queueAttr, "Exclusive") ?? false,
                    AutoDelete = GetBoolAttributeValue(queueAttr, "AutoDelete") ?? false,
                    MessageTtl = GetIntAttributeValue(queueAttr, "MessageTtl"),
                    MaxLength = GetIntAttributeValue(queueAttr, "MaxLength"),
                    DeadLetterExchange = GetAttributeArgumentValue(queueAttr, "DeadLetterExchange"),
                    DeadLetterRoutingKey = GetAttributeArgumentValue(queueAttr, "DeadLetterRoutingKey"),
                    Arguments = GetAttributeArgumentValue(queueAttr, "Arguments"),
                });
            }
        }

        return result;
    }

    /// <summary>
    /// 从类的 [RabbitMqBinding] 特性列表中提取所有绑定声明信息。
    /// </summary>
    private static List<BindingDeclarationInfo> GetBindingDeclarations(ClassDeclarationSyntax classDecl)
    {
        var result = new List<BindingDeclarationInfo>();

        foreach (var attr in GetAttributes(classDecl))
        {
            if (attr.Name.ToString().Contains("RabbitMqBinding") && attr is AttributeSyntax bindingAttr)
            {
                result.Add(new BindingDeclarationInfo
                {
                    Exchange = GetRequiredAttributeArgumentValue(bindingAttr, "Exchange"),
                    RoutingKey = GetRequiredAttributeArgumentValue(bindingAttr, "RoutingKey"),
                    QueueName = GetAttributeArgumentValue(bindingAttr, "QueueName"),
                });
            }
        }

        return result;
    }

    /// <summary>
    /// 从类的所有方法中提取标记了 [RabbitMqSubscribe] 或 [RabbitMqBatchSubscribe] 特性的方法信息。
    /// </summary>
    private static List<MethodSubscriptionInfo> GetMethodSubscriptions(ClassDeclarationSyntax classDecl)
    {
        var result = new List<MethodSubscriptionInfo>();

        foreach (var member in classDecl.Members.OfType<MethodDeclarationSyntax>())
        {
            foreach (var attrList in member.AttributeLists)
            {
                foreach (var attr in attrList.Attributes)
                {
                    var attrName = attr.Name.ToString();
                    bool isSubscribe = attrName.Contains("RabbitMqSubscribe") && !attrName.Contains("Batch");
                    bool isBatchSubscribe = attrName.Contains("RabbitMqBatchSubscribe");

                    if ((isSubscribe || isBatchSubscribe) && attr is AttributeSyntax subscribeAttr)
                    {
                        var methodInfo = new MethodSubscriptionInfo
                        {
                            ClassName = classDecl.Identifier.Text,
                            MethodName = member.Identifier.Text,
                            Namespace = GetNamespace(classDecl),
                            FullName = $"{GetFullName(classDecl)}.{member.Identifier.Text}",
                            ReturnType = member.ReturnType.ToString(),
                            QueueName = GetAttributeArgumentValue(subscribeAttr, "Queue") ?? string.Empty,
                            Exchange = GetAttributeArgumentValue(subscribeAttr, "Exchange"),
                            RoutingKey = GetAttributeArgumentValue(subscribeAttr, "RoutingKey"),
                            ConnectionName = GetAttributeArgumentValue(subscribeAttr, "ConnectionName"),
                            AutoAck = GetBoolAttributeValue(subscribeAttr, "AutoAck") ?? false,
                            PrefetchCount = GetUshortAttributeValue(subscribeAttr, "PrefetchCount") ?? 10,
                            Durable = GetBoolAttributeValue(subscribeAttr, "Durable") ?? true,
                            MaxRetryCount = GetIntAttributeValue(subscribeAttr, "MaxRetryCount") ?? 3,
                            IsBatch = isBatchSubscribe,
                            BatchSize = isBatchSubscribe ? GetIntAttributeValue(subscribeAttr, "BatchSize") ?? 10 : null,
                            BatchTimeoutMs = isBatchSubscribe ? GetIntAttributeValue(subscribeAttr, "BatchTimeoutMs") ?? 1000 : null,
                        };

                        result.Add(methodInfo);
                    }
                }
            }
        }

        return result;
    }

    /// <summary>
    /// 生成消费者注册扩展类的源代码，包含三个方法：
    /// AddXxx（注册消费者类本身）、AddXxxConsumer（同上别名）、AddXxxSubscriber（注册 SubscriberRegistration）。
    /// </summary>
    private static string GenerateRegistrationSource(
        string namespaceName,
        string className,
        string fullName,
        ConsumerRegistrationInfo info)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using Hyz.RabbitMQ.Abstractions;");
        sb.AppendLine("using Hyz.RabbitMQ.Subscriber;");
        sb.AppendLine("using Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine();
        sb.AppendLine($"namespace {namespaceName}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static class {className}RegistrationExtensions");
        sb.AppendLine("    {");
        sb.AppendLine($"        public static IServiceCollection Add{className}(this IServiceCollection services)");
        sb.AppendLine("        {");
        sb.AppendLine($"            services.AddScoped<{fullName}>();");
        sb.AppendLine("            return services;");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine($"        public static IServiceCollection Add{className}Consumer(this IServiceCollection services)");
        sb.AppendLine("        {");
        sb.AppendLine($"            services.AddScoped<{fullName}>();");
        sb.AppendLine("            return services;");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine($"        public static IServiceCollection Add{className}Subscriber(");
        sb.AppendLine("            this IServiceCollection services,");
        sb.AppendLine("            RabbitMqSubscriberOptions? options = null)");
        sb.AppendLine("        {");
        sb.AppendLine($"            services.AddScoped<{fullName}>();");
        sb.AppendLine("            services.AddSingleton(new SubscriberRegistration");
        sb.AppendLine("            {");
        sb.AppendLine($"                Name = \"{className}\",");
        sb.AppendLine($"                QueueName = \"{info.QueueName}\",");

        if (!string.IsNullOrEmpty(info.Exchange))
            sb.AppendLine($"                ExchangeName = \"{info.Exchange}\",");

        if (!string.IsNullOrEmpty(info.RoutingKey))
            sb.AppendLine($"                RoutingKey = \"{info.RoutingKey}\",");

        if (!string.IsNullOrEmpty(info.ConnectionName))
            sb.AppendLine($"                ConnectionName = \"{info.ConnectionName}\",");

        sb.AppendLine($"                ConsumerType = typeof({fullName}),");
        sb.AppendLine($"                PrefetchCount = {(ushort)info.PrefetchCount},");
        sb.AppendLine($"                EnableRetry = {(info.MaxRetryCount > 0).ToString().ToLowerInvariant()},");
        sb.AppendLine($"                MaxRetryCount = {info.MaxRetryCount},");

        if (!string.IsNullOrEmpty(info.DeadLetterExchange))
            sb.AppendLine($"                // DeadLetterExchange = \"{info.DeadLetterExchange}\",");

        if (!string.IsNullOrEmpty(info.DeadLetterRoutingKey))
            sb.AppendLine($"                // DeadLetterRoutingKey = \"{info.DeadLetterRoutingKey}\",");

        sb.AppendLine("            });");
        sb.AppendLine("            return services;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    /// <summary>
    /// 生成交换机/队列/绑定声明基础设施扩展类的源代码。
    /// 提供 DeclareXxxInfrastructureAsync 方法，在应用启动时调用以确保 RabbitMQ 拓扑已就绪。
    /// </summary>
    private static string GenerateInfrastructureSource(
        string namespaceName,
        string className,
        List<ExchangeDeclarationInfo> exchanges,
        List<QueueDeclarationInfo> queues,
        List<BindingDeclarationInfo> bindings)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using Hyz.RabbitMQ.Abstractions;");
        sb.AppendLine("using Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine();
        sb.AppendLine($"namespace {namespaceName}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static class {className}InfrastructureExtensions");
        sb.AppendLine("    {");
        sb.AppendLine($"        public static async Task Declare{className}InfrastructureAsync(");
        sb.AppendLine("            this IServiceProvider serviceProvider,");
        sb.AppendLine("            string connectionName = \"Default\",");
        sb.AppendLine("            CancellationToken cancellationToken = default)");
        sb.AppendLine("        {");
        sb.AppendLine("            using var scope = serviceProvider.CreateScope();");
        sb.AppendLine("            var connectionManager = scope.ServiceProvider.GetRequiredService<Hyz.RabbitMQ.Abstractions.IConnectionManager>();");
        sb.AppendLine("            var provider = connectionManager.GetProvider(connectionName);");

        if (exchanges.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("            // Declare exchanges");
            foreach (var exchange in exchanges)
            {
                sb.AppendLine($"            await provider.ExchangeManager.DeclareAsync(");
                sb.AppendLine($"                \"{exchange.Name}\",");
                sb.AppendLine($"                exchangeType: Hyz.RabbitMQ.Abstractions.ExchangeType.{exchange.Type},");
                sb.AppendLine($"                durable: {exchange.Durable.ToString().ToLowerInvariant()},");
                sb.AppendLine($"                autoDelete: {exchange.AutoDelete.ToString().ToLowerInvariant()},");

                if (!string.IsNullOrEmpty(exchange.Arguments))
                    sb.AppendLine($"                arguments: System.Text.Json.JsonSerializer.Deserialize<System.Collections.Generic.Dictionary<string, object?>>(\"{exchange.Arguments}\"),");
                else
                    sb.AppendLine("                arguments: null,");

                sb.AppendLine("                cancellationToken);");
            }
        }

        if (queues.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("            // Declare queues");
            foreach (var queue in queues)
            {
                var argsList = new List<string>();

                if (queue.MessageTtl.HasValue)
                    argsList.Add($"\"x-message-ttl\": {queue.MessageTtl.Value}");

                if (queue.MaxLength.HasValue)
                    argsList.Add($"\"x-max-length\": {queue.MaxLength.Value}");

                if (!string.IsNullOrEmpty(queue.DeadLetterExchange))
                    argsList.Add($"\"x-dead-letter-exchange\": \"{queue.DeadLetterExchange}\"");

                if (!string.IsNullOrEmpty(queue.DeadLetterRoutingKey))
                    argsList.Add($"\"x-dead-letter-routing-key\": \"{queue.DeadLetterRoutingKey}\"");

                if (!string.IsNullOrEmpty(queue.Arguments))
                {
                    var customArgs = queue.Arguments.Trim();
                    if (customArgs.StartsWith("{") && customArgs.EndsWith("}"))
                    {
                        sb.AppendLine($"            var queueArgs = new System.Collections.Generic.Dictionary<string, object?>");
                        sb.AppendLine("            {");
                        foreach (var arg in argsList)
                        {
                            var parts = arg.Split(new [] {':', ' '}, System.StringSplitOptions.RemoveEmptyEntries);
                            sb.AppendLine($"                {{ {parts[0]}, {parts[1]} }},");
                        }
                        sb.AppendLine($"            }};");
                    }
                }

                sb.AppendLine($"            await provider.QueueManager.DeclareAsync(");
                sb.AppendLine($"                \"{queue.Name}\",");
                sb.AppendLine($"                durable: {queue.Durable.ToString().ToLowerInvariant()},");
                sb.AppendLine($"                exclusive: {queue.Exclusive.ToString().ToLowerInvariant()},");
                sb.AppendLine($"                autoDelete: {queue.AutoDelete.ToString().ToLowerInvariant()},");

                if (argsList.Count > 0 || !string.IsNullOrEmpty(queue.Arguments))
                    sb.AppendLine("                arguments: queueArgs,");
                else
                    sb.AppendLine("                arguments: null,");

                sb.AppendLine("                cancellationToken);");
            }
        }

        if (bindings.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("            // Declare bindings");
            foreach (var binding in bindings)
            {
                var queueName = string.IsNullOrEmpty(binding.QueueName)
                    ? (queues.Count > 0 ? queues[0].Name : throw new InvalidOperationException($"Binding to exchange '{binding.Exchange}' requires a queue. Add [RabbitMqQueue] attribute to the class or specify QueueName in [RabbitMqBinding]."))
                    : binding.QueueName;
                sb.AppendLine($"            await provider.ExchangeManager.BindAsync(");
                sb.AppendLine($"                exchangeName: \"{binding.Exchange}\",");
                sb.AppendLine($"                queueName: \"{queueName}\",");
                sb.AppendLine($"                routingKey: \"{binding.RoutingKey}\",");
                sb.AppendLine($"                cancellationToken: cancellationToken);");
            }
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    /// <summary>
    /// 为每个订阅方法生成独立的处理扩展类。
    /// </summary>
    private static string GenerateHandlerExtension(
        string namespaceName,
        string className,
        MethodSubscriptionInfo info)
    {
        // 使用类名+方法名避免多个方法订阅时的类名冲突
        var extensionClassName = $"{className}_{info.MethodName}_HandlerExtensions";
        // 注册的是类本身，不是方法名
        var classFullName = string.IsNullOrEmpty(info.Namespace)
            ? info.ClassName
            : $"{info.Namespace}.{info.ClassName}";

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();
        sb.AppendLine("using Hyz.RabbitMQ.Abstractions;");
        sb.AppendLine("using Microsoft.Extensions.DependencyInjection;");
        sb.AppendLine();
        sb.AppendLine($"namespace {namespaceName}");
        sb.AppendLine("{");
        sb.AppendLine($"    public static class {extensionClassName}");
        sb.AppendLine("    {");
        sb.AppendLine($"        public static IServiceCollection Add{info.MethodName}Handler(this IServiceCollection services)");
        sb.AppendLine("        {");
        sb.AppendLine($"            services.AddScoped<{classFullName}>();");
        sb.AppendLine("            return services;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    /// <summary>
    /// 获取类声明所在的命名空间。
    /// 支持常规 NamespaceDeclarationSyntax 和 File-Scoped NamespaceDeclarationSyntax 两种形式。
    /// </summary>
    private static string GetNamespace(ClassDeclarationSyntax classDecl)
    {
        if (classDecl.Parent is NamespaceDeclarationSyntax namespaceDecl)
            return namespaceDecl.Name.ToString();

        if (classDecl.Parent is FileScopedNamespaceDeclarationSyntax globalNamespace)
            return globalNamespace.Name.ToString();

        return string.Empty;
    }

    /// <summary>
    /// 获取类的完全限定名（带命名空间）。
    /// </summary>
    private static string GetFullName(ClassDeclarationSyntax classDecl)
    {
        var ns = GetNamespace(classDecl);
        return string.IsNullOrEmpty(ns)
            ? classDecl.Identifier.Text
            : $"{ns}.{classDecl.Identifier.Text}";
    }

    /// <summary>
    /// 获取类声明上的所有 Attribute 列表。
    /// </summary>
    private static List<AttributeSyntax> GetAttributes(ClassDeclarationSyntax classDecl)
    {
        var result = new List<AttributeSyntax>();
        foreach (var attrList in classDecl.AttributeLists)
        {
            foreach (var attr in attrList.Attributes)
            {
                if (attr is AttributeSyntax attributeSyntax)
                {
                    result.Add(attributeSyntax);
                }
            }
        }
        return result;
    }

    /// <summary>
    /// 获取特性中指定名称的必填参数值，若不存在则抛出异常。
    /// </summary>
    private static string GetRequiredAttributeArgumentValue(AttributeSyntax attribute, string argumentName)
    {
        return GetAttributeArgumentValue(attribute, argumentName)
            ?? throw new InvalidOperationException($"Required attribute argument '{argumentName}' not found on {attribute.Name}");
    }

    /// <summary>
    /// 获取特性中指定名称的参数值（支持命名参数和位置参数）。
    /// </summary>
    private static string? GetAttributeArgumentValue(AttributeSyntax attribute, string argumentName)
    {
        // 优先按命名参数匹配（精确）
        foreach (var arg in attribute.ArgumentList?.Arguments ?? Enumerable.Empty<AttributeArgumentSyntax>())
        {
            if (arg.NameEquals != null &&
                arg.NameEquals.Name.Identifier.ValueText.Equals(argumentName, StringComparison.OrdinalIgnoreCase))
            {
                return GetLiteralValue(arg.Expression);
            }
        }

        // 退而按位置参数匹配（按声明顺序）
        foreach (var arg in attribute.ArgumentList?.Arguments ?? Enumerable.Empty<AttributeArgumentSyntax>())
        {
            if (arg.NameEquals == null)
            {
                return GetLiteralValue(arg.Expression);
            }
        }

        return null;
    }

    private static bool? GetBoolAttributeValue(AttributeSyntax attribute, string argumentName)
    {
        var value = GetAttributeArgumentValue(attribute, argumentName);
        if (value is null) return null;
        return bool.Parse(value);
    }

    private static int? GetIntAttributeValue(AttributeSyntax attribute, string argumentName)
    {
        var value = GetAttributeArgumentValue(attribute, argumentName);
        if (value is null) return null;
        return int.Parse(value);
    }

    private static ushort? GetUshortAttributeValue(AttributeSyntax attribute, string argumentName)
    {
        var value = GetAttributeArgumentValue(attribute, argumentName);
        if (value is null) return null;
        return ushort.Parse(value);
    }

    /// <summary>
    /// 从表达式语法中提取字面量值。
    /// 仅支持直接字面量（字符串、数字等），插值字符串和成员访问等返回 null。
    /// </summary>
    private static string? GetLiteralValue(ExpressionSyntax? expression)
    {
        return expression switch
        {
            LiteralExpressionSyntax literal => literal.Token.ValueText,
            InterpolatedStringExpressionSyntax => null,
            InvocationExpressionSyntax => null,
            _ => null,
        };
    }
}

/// <summary>
/// 从 [RabbitMqConsumer] 特性中提取的消费者注册信息记录。
/// </summary>
internal record ConsumerRegistrationInfo
{
    public required string QueueName { get; init; }
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string? ConnectionName { get; init; }
    public bool AutoAck { get; init; }
    public ushort PrefetchCount { get; init; }
    public bool Durable { get; init; }
    public int MaxRetryCount { get; init; }
    public string? DeadLetterExchange { get; init; }
    public string? DeadLetterRoutingKey { get; init; }
}

/// <summary>
/// 从 [RabbitMqSubscribe] / [RabbitMqBatchSubscribe] 方法特性中提取的订阅方法信息记录。
/// </summary>
internal record MethodSubscriptionInfo
{
    public required string ClassName { get; init; }
    public required string MethodName { get; init; }
    public required string Namespace { get; init; }
    public required string FullName { get; init; }
    public required string ReturnType { get; init; }
    public string? QueueName { get; init; }
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string? ConnectionName { get; init; }
    public bool AutoAck { get; init; }
    public ushort PrefetchCount { get; init; }
    public bool Durable { get; init; }
    public int MaxRetryCount { get; init; }
    public bool IsBatch { get; init; }
    public int? BatchSize { get; init; }
    public int? BatchTimeoutMs { get; init; }
}

/// <summary>
/// 从 [RabbitMqExchange] 特性中提取的交换机声明信息记录。
/// </summary>
internal record ExchangeDeclarationInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public bool Durable { get; init; }
    public bool AutoDelete { get; init; }
    public string? Arguments { get; init; }
}

/// <summary>
/// 从 [RabbitMqQueue] 特性中提取的队列声明信息记录。
/// </summary>
internal record QueueDeclarationInfo
{
    public required string Name { get; init; }
    public bool Durable { get; init; }
    public bool Exclusive { get; init; }
    public bool AutoDelete { get; init; }
    public int? MessageTtl { get; init; }
    public int? MaxLength { get; init; }
    public string? DeadLetterExchange { get; init; }
    public string? DeadLetterRoutingKey { get; init; }
    public string? Arguments { get; init; }
}

/// <summary>
/// 从 [RabbitMqBinding] 特性中提取的绑定声明信息记录。
/// </summary>
internal record BindingDeclarationInfo
{
    public required string Exchange { get; init; }
    public required string RoutingKey { get; init; }
    public string? QueueName { get; init; }
}
