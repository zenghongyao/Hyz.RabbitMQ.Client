using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Hyz.RabbitMQ.Generator;

/// <summary>
/// 源代码生成器的语法接收器，负责在编译时遍历语法树，
/// 收集所有标记了 RabbitMQ 相关特性（Attribute）的类和方法。
///
/// <para>实现 <see cref="ISyntaxReceiver"/> 接口，编译过程中 Roslyn 会调用
/// <see cref="OnVisitSyntaxNode"/> 方法访问语法树中的每个节点。</para>
///
/// <para>收集逻辑：</para>
/// <list type="number">
///   <item>如果访问到类声明节点，检查其是否标记了 [RabbitMqConsumer] 特性，是则加入 Consumers 列表。</item>
///   <item>如果访问到方法声明节点，检查其是否标记了 [RabbitMqSubscribe] 或 [RabbitMqBatchSubscribe] 特性，
///        若为是则将所在类也加入 Consumers 列表（以确保生成器处理该类）。</item>
/// </list>
/// </summary>
internal class ConsumerSyntaxReceiver : ISyntaxReceiver
{
    /// <summary>
    /// 收集到的所有需要生成代码的消费者类声明列表。
    /// 每个条目均为 ClassDeclarationSyntax，可在生成器 Execute 阶段使用 Roslyn API 进一步分析。
    /// </summary>
    public List<ClassDeclarationSyntax> Consumers { get; } = new();

    /// <summary>
    /// Roslyn 在遍历语法树时，对每个节点调用此方法。
    /// 通过检查节点类型和特性标记来决定是否收集。
    /// </summary>
    /// <param name="syntaxNode">当前访问的语法节点。</param>
    public void OnVisitSyntaxNode(SyntaxNode syntaxNode)
    {
        // 收集标记了 [RabbitMqConsumer] 的类
        if (syntaxNode is ClassDeclarationSyntax classDecl)
        {
            foreach (var attrList in classDecl.AttributeLists)
            {
                foreach (var attr in attrList.Attributes)
                {
                    if (attr.Name.ToString().Contains("RabbitMqConsumer"))
                    {
                        Consumers.Add(classDecl);
                        return;
                    }
                }
            }
        }

        // 收集包含 [RabbitMqSubscribe] / [RabbitMqBatchSubscribe] 方法的类
        if (syntaxNode is MethodDeclarationSyntax methodDecl)
        {
            foreach (var attrList in methodDecl.AttributeLists)
            {
                foreach (var attr in attrList.Attributes)
                {
                    var attrName = attr.Name.ToString();
                    if (attrName.Contains("RabbitMqSubscribe") || attrName.Contains("RabbitMqBatchSubscribe"))
                    {
                        // 方法必须在类中，获取其父类并收集
                        if (methodDecl.Parent is ClassDeclarationSyntax parentClass)
                        {
                            if (!Consumers.Contains(parentClass))
                            {
                                Consumers.Add(parentClass);
                            }
                        }
                        return;
                    }
                }
            }
        }
    }
}
