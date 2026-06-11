namespace Hyz.RabbitMQ.Core;

/// <summary>
/// 重试延迟计算器
/// </summary>
public static class RetryDelayCalculator
{
    /// <summary>
    /// 计算重试延迟
    /// </summary>
    /// <param name="retryCount">当前重试次数 (从 1 开始)</param>
    /// <param name="baseDelayMs">基础延迟 (毫秒)</param>
    /// <param name="minDelayMs">最小延迟 (毫秒)</param>
    /// <param name="maxDelayMs">最大延迟 (毫秒)</param>
    /// <param name="strategy">退避策略</param>
    /// <returns>计算的延迟时间 (毫秒)</returns>
    public static int Calculate(
        int retryCount,
        int baseDelayMs,
        int minDelayMs,
        int maxDelayMs,
        Abstractions.RetryBackoffStrategy strategy)
    {
        if (retryCount <= 0)
            return baseDelayMs;

        int delay = strategy switch
        {
            Abstractions.RetryBackoffStrategy.Fixed => baseDelayMs,
            Abstractions.RetryBackoffStrategy.Linear => baseDelayMs * retryCount,
            Abstractions.RetryBackoffStrategy.Exponential => (int)Math.Min(baseDelayMs * Math.Pow(2, retryCount - 1), maxDelayMs),
            _ => baseDelayMs
        };

        return Math.Max(delay, minDelayMs);
    }

    /// <summary>
    /// 根据选项计算重试延迟
    /// </summary>
    public static int Calculate(
        int retryCount,
        Abstractions.RabbitMqConnectionOptions options)
    {
        return Calculate(
            retryCount,
            options.RetryDelayMs,
            options.MinRetryDelayMs,
            options.MaxRetryDelayMs,
            options.BackoffStrategy);
    }
}
