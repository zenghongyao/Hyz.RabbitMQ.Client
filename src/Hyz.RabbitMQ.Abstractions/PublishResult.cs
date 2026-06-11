namespace Hyz.RabbitMQ.Abstractions;

/// <summary>
/// 发布结果
/// </summary>
public class PublishResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool IsSuccess { get; init; }

    /// <summary>
    /// 消息序列号
    /// </summary>
    public ulong SequenceNumber { get; init; }

    /// <summary>
    /// 错误信息
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// 确认时间
    /// </summary>
    public DateTimeOffset? ConfirmedAt { get; init; }

    /// <summary>
    /// 创建成功结果
    /// </summary>
    public static PublishResult Success(ulong sequenceNumber) => new()
    {
        IsSuccess = true,
        SequenceNumber = sequenceNumber,
        ConfirmedAt = DateTimeOffset.UtcNow
    };

    /// <summary>
    /// 创建失败结果
    /// </summary>
    public static PublishResult Failure(string errorMessage) => new()
    {
        IsSuccess = false,
        ErrorMessage = errorMessage
    };
}

/// <summary>
/// 批量发布结果
/// </summary>
public class BatchPublishResult
{
    /// <summary>
    /// 成功发布的消息数
    /// </summary>
    public int SuccessCount { get; init; }

    /// <summary>
    /// 失败的消息数
    /// </summary>
    public int FailedCount { get; init; }

    /// <summary>
    /// 失败详情
    /// </summary>
    public IReadOnlyList<BatchPublishFailure> Failures { get; init; } = [];

    /// <summary>
    /// 总耗时
    /// </summary>
    public TimeSpan Elapsed { get; init; }

    /// <summary>
    /// 是否全部成功
    /// </summary>
    public bool IsAllSuccess => FailedCount == 0;
}

/// <summary>
/// 批量发布失败详情
/// </summary>
public record BatchPublishFailure(int Index, string? RoutingKey, Exception Exception);
