using Hyz.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hyz.RabbitMQ.Subscriber;

/// <summary>
/// 订阅扫描宿主服务 - 自动管理所有消费者线程
/// </summary>
public class RabbitMqSubscriberHost : IHostedService, IDisposable
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IEnumerable<SubscriberRegistration> _registrations;
    private readonly RabbitMqSubscriberOptions _options;
    private readonly ILogger<RabbitMqSubscriberHost> _logger;
    private readonly List<SubscriptionWorker> _workers = new();
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private bool _isStarted;
    private CancellationTokenSource? _cts;

    /// <summary>
    /// 创建订阅宿主
    /// </summary>
    public RabbitMqSubscriberHost(
        IServiceProvider serviceProvider,
        IEnumerable<SubscriberRegistration> registrations,
        RabbitMqSubscriberOptions? options = null,
        ILogger<RabbitMqSubscriberHost>? logger = null)
    {
        _serviceProvider = serviceProvider;
        _registrations = registrations;
        _options = options ?? new RabbitMqSubscriberOptions();
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RabbitMqSubscriberHost>.Instance;
    }

    /// <summary>
    /// 启动所有订阅
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        await _startLock.WaitAsync(cancellationToken);
        try
        {
            if (_isStarted)
            {
                _logger.LogWarning("订阅宿主已经启动");
                return;
            }

            _logger.LogInformation("开始扫描并启动订阅线程...");

            _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            var sortedRegistrations = _registrations
                .OrderBy(r => r.StartupPriority)
                .ToList();

            foreach (var registration in sortedRegistrations)
            {
                var worker = new SubscriptionWorker(registration, _serviceProvider, _options);
                _workers.Add(worker);

                _logger.LogInformation("扫描到消费者: {Name} (连接: {Connection}, 队列: {Queue})",
                    registration.Name, registration.ConnectionName, registration.QueueName);

                _ = worker.StartAsync(_cts.Token);

                if (_options.StartupDelay > TimeSpan.Zero)
                {
                    await Task.Delay(_options.StartupDelay, cancellationToken);
                }
            }

            _isStarted = true;
            _logger.LogInformation("所有订阅线程启动完成 (共 {Count} 个)", _workers.Count);
        }
        finally
        {
            _startLock.Release();
        }
    }

    /// <summary>
    /// 停止所有订阅
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("正在停止所有订阅线程...");

        _cts?.Cancel();

        await Task.WhenAll(_workers.Select(w => w.StopAsync(cancellationToken)));

        _workers.Clear();
        _isStarted = false;

        _logger.LogInformation("所有订阅线程已停止");
    }

    /// <summary>
    /// 获取所有订阅状态
    /// </summary>
    public IReadOnlyList<SubscriptionStatus> GetAllStatuses()
    {
        return _workers.Select(w => w.GetStatus()).ToList();
    }

    /// <summary>
    /// 释放主机资源：取消所有订阅线程并释放内部 CancellationTokenSource。
    /// </summary>
    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _startLock.Dispose();
        GC.SuppressFinalize(this);
    }
}
