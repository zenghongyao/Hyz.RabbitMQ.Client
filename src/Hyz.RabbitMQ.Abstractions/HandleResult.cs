namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 处理结果
/// </summary>
public class HandleResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool IsSuccess { get; init; }

    /// <summary>
    /// 是否重新入队
    /// </summary>
    public bool Requeue { get; init; }

    /// <summary>
    /// 错误信息
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// 重试次数
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// 成功结果
    /// </summary>
    public static HandleResult SuccessResult => new() { IsSuccess = true };

    /// <summary>
    /// 失败并拒绝
    /// </summary>
    public static HandleResult Reject(string? errorMessage = null) => new()
    {
        IsSuccess = false,
        Requeue = false,
        ErrorMessage = errorMessage
    };

    /// <summary>
    /// 失败并重试
    /// </summary>
    public static HandleResult Retry(string? errorMessage = null, int retryCount = 1) => new()
    {
        IsSuccess = false,
        Requeue = true,
        ErrorMessage = errorMessage,
        RetryCount = retryCount
    };
}

/// <summary>
/// 批量处理结果
/// </summary>
public class BatchHandleResult
{
    /// <summary>
    /// 成功处理的消息数
    /// </summary>
    public int SuccessCount { get; init; }

    /// <summary>
    /// 需要重试的消息索引
    /// </summary>
    public IReadOnlyList<int> RetryIndices { get; init; } = [];

    /// <summary>
    /// 需要拒绝的消息索引
    /// </summary>
    public IReadOnlyList<int> RejectIndices { get; init; } = [];

    /// <summary>
    /// 创建全部成功的结果
    /// </summary>
    public static BatchHandleResult AllSuccess(int count) => new() { SuccessCount = count };

    /// <summary>
    /// 创建部分成功的结果
    /// </summary>
    public static BatchHandleResult Partial(
        int successCount,
        IEnumerable<int>? retryIndices = null,
        IEnumerable<int>? rejectIndices = null) => new()
        {
            SuccessCount = successCount,
            RetryIndices = (retryIndices?.ToList() ?? []),
            RejectIndices = (rejectIndices?.ToList() ?? [])
        };
}
