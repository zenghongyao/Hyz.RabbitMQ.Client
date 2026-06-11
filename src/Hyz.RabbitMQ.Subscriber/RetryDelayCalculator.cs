namespace Hyz.RabbitMQ.Subscriber;

/// <summary>
/// 重试延迟计算器
/// </summary>
public static class RetryDelayCalculator
{
    /// <summary>
    /// 最大延迟上限（毫秒），防止指数退避计算溢出
    /// </summary>
    public const long MaxDelayMs = long.MaxValue / 2;

    /// <summary>
    /// 计算重试延迟（毫秒）
    /// </summary>
    /// <param name="attempt">重试次数（从 1 开始）</param>
    /// <param name="strategy">重试策略</param>
    /// <param name="baseDelayMs">基础延迟（毫秒）</param>
    /// <param name="maxDelayMs">可选的最大延迟上限，默认使用 MaxDelayMs</param>
    /// <returns>应等待的延迟（毫秒），不会超过最大延迟</returns>
    public static long CalculateDelay(int attempt, RetryStrategy strategy, long baseDelayMs, long? maxDelayMs = null)
    {
        if (attempt < 1)
            throw new ArgumentOutOfRangeException(nameof(attempt), "Attempt must be >= 1");
        if (baseDelayMs < 0)
            throw new ArgumentOutOfRangeException(nameof(baseDelayMs), "Base delay must be >= 0");

        var effectiveMaxDelay = maxDelayMs ?? MaxDelayMs;
        if (effectiveMaxDelay < 0)
            throw new ArgumentOutOfRangeException(nameof(maxDelayMs), "Max delay must be >= 0");

        var delay = strategy switch
        {
            RetryStrategy.Fixed => baseDelayMs,
            RetryStrategy.Linear => Math.Min(baseDelayMs * attempt, effectiveMaxDelay),
            RetryStrategy.Exponential => Math.Min(baseDelayMs * (1L << Math.Min(attempt - 1, 62)), effectiveMaxDelay),
            _ => baseDelayMs
        };

        return delay;
    }
}
