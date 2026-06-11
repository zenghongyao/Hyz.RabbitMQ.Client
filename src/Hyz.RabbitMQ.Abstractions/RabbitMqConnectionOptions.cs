namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// RabbitMQ 连接配置选项
/// </summary>
public class RabbitMqConnectionOptions
{
    /// <summary>
    /// 连接名称 (不填则使用默认名称)
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// 主机地址
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// 端口号
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// 用户名
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// 密码
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// 虚拟主机
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// 连接超时时间(毫秒)
    /// </summary>
    public int ConnectionTimeout { get; set; } = 30000;

    /// <summary>
    /// 心跳间隔(秒)
    /// </summary>
    public ushort Heartbeat { get; set; } = 60;

    /// <summary>
    /// 自动重连
    /// </summary>
    public bool AutoReconnect { get; set; } = true;

    /// <summary>
    /// 最大重试次数
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 重试间隔(毫秒)
    /// </summary>
    public int RetryDelayMs { get; set; } = 5000;

    /// <summary>
    /// 重试退避策略
    /// </summary>
    public RetryBackoffStrategy BackoffStrategy { get; set; } = RetryBackoffStrategy.Exponential;

    /// <summary>
    /// 最小重试间隔(毫秒)，用于指数退避
    /// </summary>
    public int MinRetryDelayMs { get; set; } = 1000;

    /// <summary>
    /// 最大重试间隔(毫秒)，用于指数退避
    /// </summary>
    public int MaxRetryDelayMs { get; set; } = 30000;

    /// <summary>
    /// 启用 TLS
    /// </summary>
    public bool EnableTls { get; set; } = false;

    /// <summary>
    /// TLS 配置
    /// </summary>
    public TlsOptions? TlsOptions { get; set; }

    /// <summary>
    /// 获取连接名称
    /// </summary>
    public string GetConnectionName()
    {
        return Name ?? ConnectionConstants.DefaultConnectionName;
    }
}

/// <summary>
/// 重试退避策略
/// </summary>
public enum RetryBackoffStrategy
{
    /// <summary>
    /// 固定间隔
    /// </summary>
    Fixed,

    /// <summary>
    /// 线性递增
    /// </summary>
    Linear,

    /// <summary>
    /// 指数退避
    /// </summary>
    Exponential
}

/// <summary>
/// TLS 配置选项
/// </summary>
public class TlsOptions
{
    /// <summary>
    /// 服务器证书
    /// </summary>
    public string? CertPath { get; set; }

    /// <summary>
    /// 私钥路径
    /// </summary>
    public string? KeyPath { get; set; }

    /// <summary>
    /// 证书密码
    /// </summary>
    public string? CertPassphrase { get; set; }

    /// <summary>
    /// 接受自签名证书
    /// </summary>
    public bool AcceptablePolicyErrors { get; set; } = false;

    /// <summary>
    /// 检查服务器证书
    /// </summary>
    public bool CheckCertificateRevocation { get; set; } = true;
}
