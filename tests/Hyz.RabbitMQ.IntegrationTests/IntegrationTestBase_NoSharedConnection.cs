namespace Hyz.RabbitMQ.IntegrationTests;

/// <summary>
/// 不需要共享 RabbitMQ 连接的集成测试基类
/// 这些测试各自创建自己的连接
/// </summary>
public abstract class IntegrationTestBase_NoSharedConnection : IDisposable
{
    protected static string RabbitMqHost => Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";
    protected static int RabbitMqPort => int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT"), out var p) ? p : 5672;
    protected static string RabbitMqUser => Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest";
    protected static string RabbitMqPass => Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest";

    protected void EnsureConnected()
    {
        if (!IsRabbitMqAvailable())
        {
            throw new Exception(
                "SKIP: RabbitMQ Docker not available. " +
                "Start: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management");
        }
    }

    protected static bool IsRabbitMqAvailable()
    {
        if (Environment.GetEnvironmentVariable("SKIP_DOCKER_TESTS") == "1")
            return false;

        try
        {
            var psi = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "ps --filter name=rabbitmq --format {{.Names}}",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var process = System.Diagnostics.Process.Start(psi);
            if (process == null) return false;
            var output = process.StandardOutput.ReadToEnd().Trim();
            process.WaitForExit(5000);
            return process.ExitCode == 0 && output.Contains("rabbitmq");
        }
        catch
        {
            return false;
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
    }
}
