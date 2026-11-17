# Pi 投机执行与显式子计算复用

提前执行工具，并通过输入证明复用已完成的成果。进程内步骤和外部程序共用资源版本、CAS、持久化存储与计算计时。

## 接入

运行环境为 Node.js ≥ 22.19；仓库的 Pi SDK 回归基线为 0.84.1。

```sh
npm ci
npm run check
npm run build
pi -e /absolute/path/to/pi-speculative-action
```

开发构建需要 `devDependencies` 中的 TypeScript。发布包包含 `dist/`，运行时加载编译后的入口。

运行 `/speculative-action` 调整预测来源和缓存开关。`enabled` 控制提前生产，默认关闭；`reuseEnabled` 控制成果复用，默认开启。

自定义工具使用 `defineSpeculativeTool`，并通过 `registerSpeculativeTool(pi, definition)` 注册。覆盖同名基础工具时，把该注册扩展放在本扩展之前。宿主适配通过 Pi 的公开注册接口完成。

```ts
import path from "node:path";
import { defineSpeculativeTool, registerSpeculativeTool } from "@earendil-works/pi-speculative-action";
import { Type } from "@earendil-works/pi-ai";

export default function extension(pi) {
  registerSpeculativeTool(pi, defineSpeculativeTool({
    name: "count_bytes", label: "Count bytes", description: "Count bytes in a file",
    parameters: Type.Object({ path: Type.String() }),
    version: "1", effect: "observation", steps: {},
    execute: async ({ computeStep, cwd }, input) => {
      const file = path.resolve(cwd, input.path);
      const count = await computeStep({
        namespace: "example.byte-count", version: "1", key: input.path,
        inputResources: [{ path: file, scope: "content" }],
      }, async view => (await view.readFile(file)).length);
      return { content: [{ type: "text", text: String(count) }], details: { count } };
    },
  }));
}
```

`computeStep` 接受同步或异步计算，返回结构化数据、`Buffer` 或 `Uint8Array`。结果按序列化后的真实字节数计费。实现或依赖库变化时更新 `version`；文件通过传入视图读取，其他输入放入 `key` 或声明的环境键。

分层计算继续使用 `defineSpeculativeStep`；见[工具模板](docs/speculative-tool-template.md)和[报表示例](examples/speculative-tool.ts)。修改类工具的视图提供 `prepare`、`computeStep`、`stat`、`readdir`。外层工作区分支回滚后，仅依赖基线只读输入的步骤仍可供后续调用采用。

## 统一配置

全局配置为 `<agent-dir>/speculative-action.json`，项目配置为 `.pi/speculative-action.json`。底层策略写在 `config` 中；直接使用 Host 时传入 `createSpeculativeActionHost(..., { config })`。类型、默认值及验证统一位于 [config-schema.ts](src/config-schema.ts)。

```json
{
  "enabled": false,
  "reuseEnabled": true,
  "config": {
    "explicitOnly": true,
    "subcomputation": { "maxStepResultBytes": 16777216, "persistTimelineIntervals": true },
    "filesystem": {
      "sandboxDriver": "in_memory", "excludes": [".git", "node_modules", "target"],
      "timestampQuietMs": 2000, "timestampQuietByFilesystem": { "0xef53": 10 }
    },
    "processReuse": {
      "actorObservationMode": "none", "persistCompletedCertificates": true,
      "environmentMode": "exact", "replayAcceptedTaints": []
    }
  }
}
```

`explicitOnly` 默认开启。内置预测预设和自由文本路径推测已移除；显式计划、结构化观测及声明的步骤仍可使用。

预测器使用序列 PPM 和结构化字段绑定。旧版 `transform/template/splice/rebase/near/join/coalesce` 绑定已停止执行，旧学习状态按分析器版本隔离。工具应把后续调用所需的参数输出为结构化字段。

环境契约支持 `exact`、`allowlist`、`denylist`、`isolated`。投影列表由 `environmentKeys` 提供，隔离环境由 `isolatedEnvironment` 提供。默认比较完整环境。`replayAcceptedTaints` 默认空列表；用户可显式允许负载所需的 `clock`、`random`、`pid_observation`、`descriptor_observation` 等标记。

`nativeBuild` 容量用于编译 Linux 助手。调整后将同一份 `config` 对象保存为 JSON，执行 `npm run setup:linux -- --config /path/to/config.json`，再重启执行域。运行时核对源码和编译配置指纹。

## 外部程序协议

在 Host 管理的工具调用中调用 `await subcomputeEnvironment()`，把返回的 `PI_SUBCOMPUTE_SOCKET` 与 `PI_SUBCOMPUTE_CONTEXT` 传给子进程。Windows 使用 Named Pipe，Linux 使用 Unix socket。设置 `subcomputation.exposeToControlledProcesses: true` 可向受控进程自动注入这些变量。

每个连接使用换行分隔的 JSON，执行一次查询及至多一次发布：

```json
{"protocol":1,"op":"compute","context":"调用令牌","namespace":"example.transform","version":"1","key":"job","inputResources":[{"path":"input.bin","scope":"content"}],"envKeys":["MODE"],"environment":{"MODE":"fast"}}
```

命中返回 `state: "hit"` 和 `value`。未命中返回 `state: "miss"` 及已捕获的 `inputResources`；外部程序使用这些输入计算，再发送：

```json
{"op":"publish","value":{"encoding":"base64","value":"YWJj"},"durationMs":12.5}
```

结构化输出使用 `{"encoding":"json","value":...}`。发布完成返回 `state: "published"`；错误返回 `state: "error"`。令牌随工具调用结束或超时撤销。示例见 [JavaScript 客户端](test/subcompute-client.mjs)和 [Python 客户端](test/subcompute-client.py)。

## Linux 进程复用

```sh
npm run setup:linux
sudo ~/.local/bin/pi-speculative-domain --user "$(id -u)" env PATH="$PATH" "$(command -v pi)"
```

执行域需要 cgroup v2、内核 BTF、eBPF tracing/kprobe hooks 和 `bpf_loop`。构建依赖 Clang、libbpf、libelf、zlib 及安装脚本报告的工具。使用 Linux 原生文件系统上的 checkout。

Linux 后端保留已完成进程的输出和可验证效果，并发布 SQLite L2 证书。`actorObservationMode: "provenance"` 为真实 Actor 执行采集依赖；当前 Actor 沉淀支持可完整验证的只读命令。写入、外部状态或不完整观测会阻止该次证书发布。

Actor 观测与普通受控执行共享子进程采用通道。纯字节输出的子进程可以多次重放，环境和非确定性接纳范围由显式契约控制。内核观测容量位于 `nativeBuild`；观测历史丢失时诊断报告 `observer_history_incomplete` 及原因位。

标准输出能否跨 pipe/socket 载体复用，由真实 syscall 观测决定。程序探测标准流元数据时，证书保留载体约束。受控异步启动使用 `createControlledBashOperations`。子进程观察另需合格的 `PI_SPEC_HELD_EXEC_OBSERVER`，安装脚本和环境诊断会报告当前资格。

进程中途续接、OFD 图迁移和可恢复进程镜像已移除。

## Windows 文件资源复用

Windows 使用显式子计算协议及文件资源复用。文件租约由 Python 3 标准库 `ctypes` 调用 `OpenFileById` 持有，支持文件和父目录重命名、删除后身份校验及硬链接。运行环境需要可执行的 Python 3，默认命令为 `python`；可通过 `filesystem.windowsObjectLeaseCommand` 指定，例如 `["py", "-3"]` 或 Python 的绝对路径。

辅助进程按配置共享，最后一个租约释放后退出。GC、工作线程退出和父进程管道关闭会回收句柄。`filesystem.objectLeaseTimeoutMs` 控制请求超时，`filesystem.maxWindowsLeaseRequests` 控制请求并发；批量释放会等待可用容量。实现随包附带，原有 `.node` 模块及其编译步骤已移除。

可在 Windows 直接运行文件资源与工作区提交回归：

```sh
npx vitest run test/filesystem-lease.test.ts --maxWorkers=1 --no-file-parallelism
```

## 计量与验证

`actionHitRate` 统计整动作采用；`subcomputationHits` 与 `reusedComputeMs` 统计内部计算复用。持久化步骤保留计算区间，跨轮恢复的实际消费继续计入隐藏计算量 `H`。

```sh
npm run check
npm run build
npm test -- --maxWorkers=1 --no-file-parallelism
npm run bench:check
node bench/portable-kernel.mjs
node bench/grep-captured-qualification.mjs
```

测试从公开 Host、真实 Pi 工具或本地协议触发计算。平台专属测试在对应系统执行，本机资格失败记录为失败原因。

公开入口为包根、`core`、`extension`、`pattern-aware`、`process-reuse`。详见 [Host API](src/index.ts)、[执行世界](src/execution-world.ts)、[验证口径](bench/README.md)。
