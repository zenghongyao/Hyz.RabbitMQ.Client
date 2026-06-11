# Hyz.RabbitMQ.Client

[![NuGet](https://img.shields.io/nuget/v/Hyz.RabbitMQ.Client.svg)](https://www.nuget.org/packages/Hyz.RabbitMQ.Client)
[![Target Framework](https://img.shields.io/badge/TFM-netstandard2.0%20%7C%20net8.0%20%7C%20net9.0%20%7C%20net10.0-blue)]()

一款**统一、优雅的 RabbitMQ 客户端库**，专为 .NET 打造。  
一个包，零模板代码。

---

## 特性

- 🚀 **一行注册** — `AddRabbitMq()` 即可完成所有配置
- 📮 **发布与消费** — `IPublisherService` / `IConsumerService` 开箱即用
- 🔁 **批量处理** — `PublishBatchAsync` / `ConsumeBatchAsync`，支持自定义批次大小和超时
- 🌐 **多连接管理** — 命名连接，轻松接入多节点 RabbitMQ 集群
- 🧩 **源码生成器** — 基于特性声明队列/交换机/绑定（编译时生成）
- 🔍 **订阅者扫描** — 自动发现程序集中标记了 `[RabbitMqConsumer]` 的处理器
- 🎯 **IAsyncEnumerable** — 现代化 `await foreach` 消费方式，告别回调地狱
- 🔄 **自动重连** — 内置指数/线性/固定退避策略
- 🏗️ **拓扑管理** — `IExchangeManager` / `IQueueManager` 运行时管理交换机、队列、绑定
- ✅ **发布确认** — `PublishWithConfirmationAsync` 确保消息可靠投递

---

## 安装

```bash
dotnet add package Hyz.RabbitMQ.Client
```

> 支持 .NET Framework 4.6.1+, .NET Core 2.0+, .NET 8.0+。

---

## 快速开始

### 1. 注册服务

```csharp
using Hyz.RabbitMQ.Extensions;

builder.Services.AddRabbitMq(options =>
{
    options.HostName = "localhost";
    options.Port     = 5672;
    options.UserName = "guest";
    options.Password = "guest";
    options.AutoReconnect = true;
});
```

### 2. 发布消息

```csharp
var publisher = sp.GetRequiredService<IPublisherService>();
var message   = new MessageBody(Encoding.UTF8.GetBytes("Hello RabbitMQ!"));

await publisher.PublishAsync("my-queue", message);
```

### 3. 消费消息

```csharp
var consumer = sp.GetRequiredService<IConsumerService>();

await foreach (var msg in consumer.ConsumeAsync("my-queue"))
{
    var text = Encoding.UTF8.GetString(msg.Body);
    Console.WriteLine($"收到消息: {text}");
    await msg.AckAsync();
}
```

---

## 核心概念

### 发布者 (Publisher)

| API | 说明 |
|-----|------|
| `PublishAsync(queue, message)` | 发布消息到队列 |
| `PublishToExchangeAsync(exchange, routingKey, message)` | 发布消息到交换机 |
| `PublishBatchAsync(exchange, routingKey, messages)` | 批量发布（优化性能） |
| `PublishWithConfirmationAsync(...)` | 带 Broker 确认的发布 |

### 消费者 (Consumer)

| API | 说明 |
|-----|------|
| `ConsumeAsync(queue)` → `IAsyncEnumerable` | 异步流式消费 |
| `ConsumeBatchAsync(queue, batchSize, timeoutMs)` | 批量消费 |
| `StartConsumingAsync(queue, handler)` | 回调方式消费 |
| `StartBatchConsumingAsync(queue, ...)` | 回调方式批量消费 |

### MessageBody

```csharp
var body1 = new MessageBody(bytes);
var body2 = "text".ToMessageBody();
var body3 = new MessageBody(myObject, serializer);
```

### ConsumerOptions

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `ConsumerTag` | `null` | 消费者标识 |
| `AutoAck` | `false` | 是否自动确认 |
| `PrefetchCount` | `10` | 预取数量 |
| `Exclusive` | `false` | 独占消费者 |
| `Priority` | `0` | 消费者优先级 |

### PublishOptions

```csharp
var options = new PublishOptions
{
    DeliveryMode  = DeliveryModes.Persistent,
    ContentType   = "application/json",
    CorrelationId = Guid.NewGuid().ToString(),
    Priority      = 5,
    Expiration    = "60000"   // 60 秒过期
};
```

### 序列化

库内置 `SystemTextJsonSerializer`（默认）和 `MessagePackSerializer`，并可自定义序列化器。

```csharp
using Hyz.RabbitMQ.Serialization;

// 对象 → MessageBody（默认 JSON）
var body = myOrder.ToMessageBody();

// 对象 → MessageBody（使用 MessagePack）
var msgPackSerializer = new MessagePackSerializer();
var body = myOrder.ToMessageBody(msgPackSerializer);

// MessageBody → 对象
var order = body.FromMessageBody<Order>();

// 字节数组 → 对象
var order = bytes.FromMessageBody<Order>();

// 字符串 ↔ MessageBody
var body = "hello".ToMessageBodyFromString();
var text = body.ToStringContent();
```

#### 自定义序列化器

实现 `IMessageSerializer` 接口即可：

```csharp
public class ProtobufSerializer : IMessageSerializer
{
    public string ContentType => "application/x-protobuf";
    public ReadOnlyMemory<byte> Serialize<T>(T obj) where T : class => /* ... */;
    public T? Deserialize<T>(ReadOnlyMemory<byte> bytes) where T : class => /* ... */;
}
```

### 消息处理

#### ReceivedMessageContext

消费者回调中接收的消息上下文，包含完整的消息元数据：

| 属性 | 类型 | 说明 |
|------|------|------|
| `Body` | `ReadOnlyMemory<byte>` | 消息体原始字节 |
| `MessageId` | `string?` | 消息唯一标识 |
| `RoutingKey` | `string` | 消息路由键 |
| `ExchangeName` | `string` | 来源交换机 |
| `QueueName` | `string` | 来源队列 |
| `DeliveryTag` | `ulong` | RabbitMQ 投递序号（用于 Ack/Nack） |
| `Headers` | `IDictionary<string, object?>?` | 消息头 |
| `ContentType` | `string?` | 内容类型 |
| `Redelivered` | `bool` | 是否为重新投递 |
| `CorrelationId` | `string?` | 关联 ID（RPC 模式） |
| `ReplyTo` | `string?` | 回复队列（RPC 模式） |
| `Priority` | `byte?` | 消息优先级 |
| `Timestamp` | `AmqpTimestamp?` | 消息时间戳 |
| `Expiration` | `string?` | 消息过期时间 |

#### 消息确认

```csharp
await foreach (var msg in consumer.ConsumeAsync("orders"))
{
    try
    {
        ProcessMessage(msg);
        await msg.AckAsync();        // 确认处理成功
    }
    catch
    {
        await msg.NackAsync(true);   // 拒绝并重新入队
    }
}
```

#### 消息处理器接口

```csharp
// 单条消息处理
public class OrderHandler : IMessageHandler
{
    public Task<HandleResult> HandleAsync(ReceivedMessageContext ctx, CancellationToken ct)
    {
        var order = ctx.Body.FromMessageBody<Order>();
        // 处理订单...
        return Task.FromResult(HandleResult.SuccessResult);
    }
}

// 批量消息处理
public class BatchOrderHandler : IBatchMessageHandler
{
    public Task<BatchHandleResult> HandleBatchAsync(
        IReadOnlyList<ReceivedMessageContext> messages, CancellationToken ct)
    {
        var orders = messages.Select(m => m.Body.FromMessageBody<Order>()).ToList();
        // 批量处理...
        return Task.FromResult(BatchHandleResult.AllSuccess(orders.Count));
    }
}
```

#### HandleResult 结果类型

| 静态方法 | 说明 |
|----------|------|
| `HandleResult.SuccessResult` | 处理成功，消息确认 |
| `HandleResult.Reject(error)` | 处理失败，消息丢弃（不进死信则可能丢失） |
| `HandleResult.Retry(error, count)` | 处理失败，消息重新入队重试 |

### 拓扑管理

`IExchangeManager` 和 `IQueueManager` 用于在运行时管理 RabbitMQ 交换机与队列，需手动实例化：

```csharp
using Hyz.RabbitMQ.Core;

var provider = sp.GetRequiredService<IConnectionProvider>();
var exchangeManager = new ExchangeManager(provider);
var queueManager = new QueueManager(provider);
```

#### 交换机管理 (IExchangeManager)

| API | 说明 |
|-----|------|
| `DeclareAsync(name, type, durable, autoDelete, args)` | 声明交换机，若不存在则创建，返回 `ExchangeInfo` |
| `DeleteAsync(name, ifUnused)` | 删除交换机（`ifUnused=true` 仅在无绑定时删除） |
| `ExistsAsync(name)` → `bool` | 检查交换机是否存在 |
| `BindAsync(exchange, queue, routingKey, args)` | 将队列绑定到交换机，指定路由键 |
| `UnbindAsync(exchange, queue, routingKey, args)` | 解除队列与交换机的绑定关系 |

```csharp
// 声明一个持久化的 Topic 交换机
var exchange = await exchangeManager.DeclareAsync(
    "shop.events",
    ExchangeType.Topic,
    durable: true);

// 绑定队列到交换机
await exchangeManager.BindAsync("shop.events", "orders", "order.created");

// 删除交换机（仅当无队列绑定时）
await exchangeManager.DeleteAsync("shop.events", ifUnused: true);
```

#### 队列管理 (IQueueManager)

| API | 说明 |
|-----|------|
| `DeclareAsync(name, durable, exclusive, autoDelete, args)` | 声明队列，返回 `QueueInfo`（含消息数、消费者数） |
| `DeleteAsync(name, ifUnused, ifEmpty)` | 删除队列，返回被删除的消息数量 |
| `PurgeAsync(name)` → `uint` | 清空队列中所有消息（不删除队列），返回清空数量 |
| `ExistsAsync(name)` → `bool` | 检查队列是否存在 |
| `GetInfoAsync(name)` → `QueueInfo` | 获取队列详情（消息数量、消费者数量） |

```csharp
// 声明持久化队列并配置死信与消息 TTL
var queue = await queueManager.DeclareAsync("orders", durable: true, arguments: new Dictionary<string, object?>
{
    ["x-dead-letter-exchange"] = "shop.dlx",
    ["x-message-ttl"] = 60000   // 消息 60 秒未消费则转死信
});
Console.WriteLine($"队列消息数: {queue.MessageCount}, 消费者: {queue.ConsumerCount}");

// 清空队列消息
var purged = await queueManager.PurgeAsync("orders");

// 删除空队列
var deleted = await queueManager.DeleteAsync("orders", ifEmpty: true);
```

#### ExchangeType 枚举

| 值 | 说明 |
|----|------|
| `Direct` | 直接交换机 — 精确匹配路由键 |
| `Fanout` | 扇出交换机 — 广播到所有绑定队列 |
| `Topic` | 主题交换机 — 通配符匹配（`*` 匹配一个词，`#` 匹配零个或多个词） |
| `Headers` | 头交换机 — 基于消息头属性匹配 |

---
## 连接配置详解

`RabbitMqConnectionOptions` 支持所有常用 RabbitMQ 连接参数：

| 属性 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `Name` | `string?` | `null` | 连接名称（用于多连接场景） |
| `HostName` | `string` | `"localhost"` | RabbitMQ 服务器地址 |
| `Port` | `int` | `5672` | 端口号 |
| `UserName` | `string` | `"guest"` | 登录用户名 |
| `Password` | `string` | `"guest"` | 登录密码 |
| `VirtualHost` | `string` | `"/"` | 虚拟主机 |
| `Heartbeat` | `ushort` | `60` | 心跳间隔（秒） |
| `ConnectionTimeout` | `int` | `30000` | 连接超时（毫秒） |
| `AutoReconnect` | `bool` | `true` | 是否自动重连 |
| `MaxRetryCount` | `int` | `3` | 最大重试次数 |
| `RetryDelayMs` | `int` | `5000` | 重试基础间隔（毫秒） |
| `BackoffStrategy` | `RetryBackoffStrategy` | `Exponential` | 重试退避策略 |
| `MinRetryDelayMs` | `int` | `1000` | 指数退避最小间隔 |
| `MaxRetryDelayMs` | `int` | `30000` | 指数退避最大间隔 |
| `EnableTls` | `bool` | `false` | 是否启用 TLS 加密 |
| `TlsOptions` | `TlsOptions?` | `null` | TLS 配置（证书路径等） |

### 重连策略

| 策略 | 说明 |
|------|------|
| `Fixed` | 固定间隔重试（每次等待 `RetryDelayMs` 毫秒） |
| `Linear` | 线性递增（第 N 次重试等待 `N × RetryDelayMs` 毫秒） |
| `Exponential` | 指数退避（每次间隔翻倍，受 `MinRetryDelayMs` / `MaxRetryDelayMs` 限制） |

### TLS 配置

```csharp
services.AddRabbitMq(options =>
{
    options.HostName = "rabbitmq.example.com";
    options.Port = 5671;
    options.EnableTls = true;
    options.TlsOptions = new TlsOptions
    {
        CertPath = "/path/to/client.p12",
        CertPassphrase = "your-password",
        CheckCertificateRevocation = true
    };
});
```

---
## 多连接管理

```csharp
// 注册多个命名连接
services.AddRabbitMq("Conn1", opts => opts.HostName = "rabbit1.local");
services.AddRabbitMq("Conn2", opts => opts.HostName = "rabbit2.local");

// 按名称获取服务
var pub1 = sp.GetRequiredKeyedService<IPublisherService>("Conn1");
var pub2 = sp.GetRequiredKeyedService<IPublisherService>("Conn2");
```

---

## 订阅者扫描

```csharp
[RabbitMqConsumer(Queue = "orders", PrefetchCount = 5)]
public class OrderHandler : IMessageHandler
{
    public Task<HandleResult> HandleAsync(ReceivedMessageContext ctx)
    {
        var text = Encoding.UTF8.GetString(ctx.Body);
        Console.WriteLine(text);
        return Task.FromResult(HandleResult.Success);
    }
}

// 扫描并启动
var host = new RabbitMqSubscriberHost(logger, connectionManager);
host.ScanAndRegister(typeof(OrderHandler).Assembly);
await host.StartAsync();
```

---

## 源码生成器

在包含生成代码的 `partial` 类上通过特性声明交换机、队列和绑定关系，编译器自动生成注册代码。

### 声明交换机、队列和绑定

```csharp
using Hyz.RabbitMQ.Abstractions.Attributes;

[RabbitMqExchange(Name = "shop", Type = "direct")]
[RabbitMqQueue(Name = "orders", Durable = true)]
[RabbitMqBinding(Exchange = "shop", RoutingKey = "order.created")]
public static partial class ShopSubscriptions
{
    [RabbitMqSubscribe(Queue = "orders")]
    public static partial Task OnOrderCreatedAsync(ReceivedMessageContext ctx);

    [RabbitMqBatchSubscribe(Queue = "batch-orders", BatchSize = 50)]
    public static partial Task OnBatchOrdersAsync(IList<ReceivedMessageContext> batch);
}
```

### 特性详解

#### [RabbitMqExchange] — 声明交换机

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `Name` | *(required)* | 交换机名称 |
| `Type` | `"Direct"` | 交换机类型（`Direct` / `Fanout` / `Topic` / `Headers`） |
| `Durable` | `true` | 是否持久化 |
| `AutoDelete` | `false` | 是否自动删除 |
| `Arguments` | `null` | 额外参数（JSON 格式） |

#### [RabbitMqQueue] — 声明队列

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `Name` | *(required)* | 队列名称 |
| `Durable` | `true` | 是否持久化 |
| `Exclusive` | `false` | 是否独占 |
| `AutoDelete` | `false` | 是否自动删除 |
| `MessageTtl` | `null` | 消息 TTL（毫秒） |
| `MaxLength` | `null` | 最大队列长度 |
| `DeadLetterExchange` | `null` | 死信交换机 |
| `DeadLetterRoutingKey` | `null` | 死信路由键 |

#### [RabbitMqBinding] — 声明绑定

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `Exchange` | *(required)* | 交换机名称 |
| `RoutingKey` | *(required)* | 路由键（支持通配符 `*` `#`） |
| `QueueName` | `null` | 队列名（不填则使用类上声明的队列） |

#### [RabbitMqSubscribe] — 单条订阅方法

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `Queue` | *(required)* | 队列名称 |
| `Exchange` | `null` | 交换机名称 |
| `RoutingKey` | `null` | 路由键 |
| `ConnectionName` | `null` | 连接名称（默认连接） |
| `AutoAck` | `false` | 自动确认 |
| `PrefetchCount` | `10` | 预取数量 |
| `Durable` | `true` | 持久化 |
| `MaxRetryCount` | `3` | 最大重试次数 |
| `DeadLetterExchange` | `null` | 死信交换机 |
| `DeadLetterRoutingKey` | `null` | 死信路由键 |
| `UseDedicatedThread` | `true` | 使用独立线程 |
| `ThreadName` | `null` | 线程名称 |
| `StartupPriority` | `0` | 启动优先级 |

#### [RabbitMqBatchSubscribe] — 批量订阅方法

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `Queue` | *(required)* | 队列名称 |
| `BatchSize` | `10` | 批量大小 |
| `BatchTimeoutMs` | `1000` | 批次超时（毫秒） |
| `Exchange` | `null` | 交换机名称 |
| `RoutingKey` | `null` | 路由键 |
| `ConnectionName` | `null` | 连接名称 |
| `PrefetchCount` | `50` | 预取数量 |

---
## 高级特性

### 死信队列 (DLX)

当消息处理失败（达到最大重试次数）或被拒绝（`requeue = false`）时，可自动路由到死信队列：

```csharp
// 订阅者扫描方式
[RabbitMqConsumer(
    Queue = "orders",
    MaxRetryCount = 3,
    DeadLetterExchange = "shop.dlx",
    DeadLetterRoutingKey = "order.failed")]
public class OrderHandler : IMessageHandler { /* ... */ }

// 源码生成器方式
[RabbitMqQueue(
    Name = "orders",
    DeadLetterExchange = "shop.dlx",
    DeadLetterRoutingKey = "order.failed")]
[RabbitMqExchange(Name = "shop.dlx", Type = "topic")]
[RabbitMqBinding(Exchange = "shop.dlx", RoutingKey = "order.failed")]
public static partial class ShopDlxSubscriptions { /* ... */ }
```

### RPC 请求/响应模式

利用 `CorrelationId` 和 `ReplyTo` 实现远程过程调用：

```csharp
// 客户端 — 发送 RPC 请求
var options = new PublishOptions
{
    CorrelationId = Guid.NewGuid().ToString(),
    ReplyTo = "rpc.reply.queue"
};
await publisher.PublishToExchangeAsync("rpc.exchange", "rpc.method", request, options);

// 服务端 — 处理并回复
await foreach (var msg in consumer.ConsumeAsync("rpc.queue"))
{
    var response = HandleRequest(msg);
    await publisher.PublishAsync(msg.ReplyTo!, response, new PublishOptions
    {
        CorrelationId = msg.CorrelationId
    });
    await msg.AckAsync();
}
```

---

## 依赖项

安装本包时会自动引入以下依赖：

| 包名 | 版本 |
|------|------|
| RabbitMQ.Client | ≥ 7.2.1 |
| MessagePack | ≥ 2.5.187 |
| Microsoft.Extensions.* | ≥ 8.0.0 |

---

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

**如果这个项目对你有帮助，请给它一个 ⭐️**
