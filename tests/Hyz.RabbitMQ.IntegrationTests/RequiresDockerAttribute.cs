namespace Hyz.RabbitMQ.IntegrationTests;

/// <summary>
/// 自定义特性：标记需要 Docker RabbitMQ 的集成测试
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
public class RequiresDockerAttribute : Attribute
{
}
