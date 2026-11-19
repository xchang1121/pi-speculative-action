# 投机工具接入模板

用 `defineSpeculativeStep` 声明可复用计算，用 `defineSpeculativeTool` 声明工具的效果边界，通过公开的 `registerSpeculativeTool(pi, tool)` 注册。扩展将契约接入资源视图、执行后端、预算、取消和回收机制。

完整可运行示例：[examples/speculative-tool.ts](../examples/speculative-tool.ts)。它注册 `inspect_dataset` 和 `write_dataset_report`，共同使用 `rows → summary → report` 三层计算。将文件放进能解析本包依赖的 Pi 扩展项目，启用本包扩展，并通过 `pi -e /absolute/path/to/speculative-tool.ts` 加载示例。输入文件内容例如 `[2,3]`；工具参数为 `path`、`multiplier`、`title`，写入工具另有 `output`。示例参与项目类型检查，并随 npm 包发布。

## 最小接入

```ts
import { Type } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { defineSpeculativeStep, defineSpeculativeTool, registerSpeculativeTool } from "@earendil-works/pi-speculative-action";

const count = defineSpeculativeStep({
  id: "my-package.count", dependencies: {},
  run: async ({ fs }, input: { path: string }) => {
    const text = (await fs.readFile(input.path)).toString("utf8");
    return text.split("\n").length;
  },
});

export default function register(pi: ExtensionAPI) {
  registerSpeculativeTool(pi, defineSpeculativeTool({
    name: "count_lines", label: "Count lines", description: "Count newline-separated segments.",
    parameters: Type.Object({ path: Type.String() }),
    effect: "observation", steps: { count },
    resources: input => [input.path],
    execute: async ({ step }, input) => {
      const value = await step("count", input);
      return { content: [{ type: "text", text: `${value}` }], details: { count: value } };
    },
  }));
}
```

## 必须遵守的计算契约

- 步骤是确定性的无副作用计算，可以同步返回，也可以 `await` 文件读取、声明过的子步骤和纯计算。所有可变配置必须进入 `input`；文件读取必须通过 `context.fs`。不要直接读 `node:fs`、环境变量、时钟、随机数、网络、数据库或可变全局变量。
- `dependencies` 声明直接子步骤；通过 `step("name", input)` 调用它们。每个子步骤只接收实际影响自己的参数，例如标题不应传给解析数据的步骤。依赖定义创建后不可修改，未知步骤名会被拒绝；静态定义自然形成无环图。
- 输入和输出支持普通对象、数组、字符串、布尔、`null`、有限数字，以及 `Buffer`/`Uint8Array`。循环、访问器、函数和资源句柄会被拒绝。每个消费者获得独立结果副本。
- 容量由顶层配置的 `subcomputation.maxStepResultBytes/maxStepObjectNodes/maxStepObjectDepth` 控制；缓存按真实序列化字节数计费。默认结果上限为 16 MiB。配置示例见 [README](../README.md)。
- `id` 标识计算契约；相同契约及参数可以共享持久化结果。影响结果的实现、依赖库或配置变化应纳入步骤的输入键。文件读取必须经过受控视图，其他影响结果的值应显式放入参数。
- 遵守 `signal`，异步长任务应响应取消，循环应主动检查 `signal.throwIfAborted()`；不要把取消状态当成业务输入，也不要启动脱离调用生命周期的后台任务。

同名工具在不同 Host 实例中使用同一份参数 schema 即可获得同一身份。工具定义可以显式提供确定性的 `identity`，例如业务规则的内容摘要。

工具更名后若仍履行同一职责，可以在新旧定义上声明相同的 `patternIdentity`，例如 `"workspace.symbols.open"`。PatternAware 将学到的调用顺序映射到当前注册名。目标参数 schema 必须一致，同一身份同时对应多个启用的目标工具时会跳过该预测。执行、证书和采纳仍使用当前工具自己的身份。

## 工具的效果边界

| `effect` | 工具作者的约定 | 运行方式 |
| --- | --- | --- |
| `observation` | 输出只由参数、声明步骤及受控文件读取决定，无外部效果 | 可提前执行，并在依赖验证通过后整步采用；文件接口只读 |
| `workspace_mutation` | 读取和写入全部通过 `context.fs`，效果限定在工作区 | 合格工作区后端可提前执行，写入在正式采用时提交；无合格后端时正常执行 |
| `actor` | 外层可能包含网络、数据库、日志等任意正式调用效果；内部步骤仍遵守纯计算契约 | 外层只在实际调用时运行，内部可消费已有步骤结果；不会提前执行外层回调 |

`execute` 返回原生 Pi 的 `{ content, details }`；异常照常成为工具错误。工具名、参数 schema、描述、显示配置等继续交给 Pi 注册。模板提供 `cwd`、`callID`、`signal`、`fs`、`step`；只有 `actor` 工具可使业务结果依赖调用编号或其他环境状态。需要完整 Pi 上下文或流式更新的现有工具，可继续使用普通注册，或在外层 Actor 适配器中安排这些行为。

`resources(input)` 是纯函数，返回潜在访问路径，用于调度顺序；应列出可能读取或写入的路径。它不代替实际读取证据，也不授予访问权限。全部路径相对当前工作区，越出根目录的路径会被拒绝。`fs` 提供 `readFile/access/exists/stat/readdir`；可写模式另有 `writeFile/mkdir`。步骤始终只拿到只读接口。`readFile(path, maxBytes)` 返回最多指定字节的前缀，需要完整内容的步骤应像完整示例一样检查长度。

只读视图和修改沙箱都提供 `prepare`、`computeStep`、`stat` 和 `readdir`。外层修改回滚后，只依赖基线只读输入的步骤继续保留；依赖私有写入的结果随分支释放。

效果留在步骤外部。例如先 `await step("report", calculationArgs)`，再 `await fs.writeFile(output, report)`。输出路径不影响报表内容时，不应放入计算参数；切换输出路径即可共享报表计算，同时仍执行本次写入。不要在可复用步骤中发送请求、写日志或保存文件，否则命中缓存时这些行为会被跳过。

## 分层复用的结果

完整示例在数据不变时具备以下行为；是否保留成果仍受容量、取消、有效性和生命周期约束。

| 本次变化 | 需要执行的计算 |
| --- | --- |
| 参数和数据都不变，换另一个共享定义的工具 | 三层均可复用 |
| 仅 `title` 变化 | 重算 `report`，复用 `summary` 与 `rows` |
| `multiplier` 变化 | 重算 `summary/report`，复用 `rows` |
| 数据文件变化 | 相关三层重新计算 |
| 仅 `output` 变化 | 复用完整报表，执行本次文件写入 |
| 关闭 Cache & reuse | 正常运行全部计算与工具效果 |

父步骤命中时不再调用或查找子步骤；父步骤未命中才自然进入其函数，逐个检查实际调用的子步骤。有效成果可以来自此前的正式调用，也可以来自受控提前执行；父子重叠收益继续按原有计算身份去重。

设置 `config.storage.root` 后，步骤结果、文件依赖与计算区间保存在 SQLite/CAS 中，新宿主重新验证依赖后可以复用。外部工具通过 `PI_SUBCOMPUTE_SOCKET` 查询和发布，协议与 Python 示例见 [README](../README.md) 及 [test/subcompute-client.py](../test/subcompute-client.py)。受控子进程需显式开启 `subcomputation.exposeToControlledProcesses`。

## 接入验收

项目的 [speculative-tool.test.ts](../test/speculative-tool.test.ts) 通过真实 Pi 会话注册模板并核对三层物理执行次数、跨工具共享、结果副本隔离、局部参数变化、文件失效、关闭复用、取消恢复，以及三类效果的提前执行与提交边界；同时检查不启用扩展时示例仍能正常运行。新工具应按自己的外部可观察结果补齐相应验收。
