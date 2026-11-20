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
    effect: "observation", steps: {},
    execute: async ({ computeStep, cwd }, input) => {
      const file = path.resolve(cwd, input.path);
      const count = await computeStep({
        namespace: "example.byte-count", key: input.path,
        inputResources: [{ path: file, scope: "content" }],
      }, async view => (await view.readFile(file)).length);
      return { content: [{ type: "text", text: String(count) }], details: { count } };
    },
  }));
}
```

`computeStep` 接受同步或异步计算，返回结构化数据、`Buffer` 或 `Uint8Array`。结果按序列化后的真实字节数计费。实现与依赖库的变化通过显式 `key` 区分；文件通过传入视图读取，其他输入放入 `key` 或声明的环境键。

分层计算继续使用 `defineSpeculativeStep`；见[工具模板](docs/speculative-tool-template.md)和[报表示例](examples/speculative-tool.ts)。修改类工具的视图提供 `prepare`、`computeStep`、`stat`、`readdir`。外层工作区分支回滚后，仅依赖基线只读输入的步骤仍可供后续调用采用。

## 统一配置

全局配置为 `<agent-dir>/speculative-action.json`，项目配置为 `.pi/speculative-action.json`。底层策略写在 `config` 中；直接使用 Host 时传入 `createSpeculativeActionHost(..., { config })`。类型、默认值及验证统一位于 [config-schema.ts](src/config-schema.ts)。

```json
{
  "enabled": false,
  "reuseEnabled": true,
  "config": {
    "subcomputation": { "maxStepResultBytes": 16777216, "persistTimelineIntervals": true },
    "filesystem": {
      "sandboxDriver": "in_memory", "excludes": [".git", "node_modules", "target"],
      "timestampQuietMs": 2000, "timestampQuietByFilesystem": { "0xef53": 10 }
    },
    "processReuse": {
      "actorObservationMode": "provenance", "persistCompletedCertificates": true,
      "environmentMode": "exact", "replayAcceptedTaints": []
    }
  }
}
```

### 执行与复用会话

PatternAware、Drafter 和 Self-Speculation 向同一会话提交额外调用。宿主可通过 `executionSession` 注入自己的实现，接口类型从包的 `/core` 入口导出。

| 方法 | 行为 |
| --- | --- |
| `speculate(call, { after?, signal? })` | 返回带有 `runID` 和 `completion` 的句柄。等价调用共享执行。新调用达到并发上限时返回 `rejected`。 |
| `execute(call, { signal? })` | 执行 Actor 调用，返回结果、复用来源和计时。Actor 可消费已有工作，也可为后续调用留下可验证的资源。 |
| `release(runID)` | 撤回调用方对分支的使用。共享执行的其他使用者可以继续等待。 |
| `flush()`、`close()` | 等待已接收的工作，或关闭会话并释放资源。 |

Drafter 在私有上下文内预测前驱的输出。独立后继可以提前执行，依赖私有写入的调用通过 `after` 绑定到前驱分支。实际输出与预期有出入时，Drafter 撤回受影响的后继并根据实际输出续推。跨回合保留的资源受输入有效性和缓存容量限制。

规范动作键相同的调用按 `exact` 复用结果。读取范围等参数变化时，运行时可以用已封存的资源输入重建并校验本次结果，计为 `inputs`。

PatternAware 用序列 PPM 和结构化字段绑定预测调用。当前台模式对本轮动作的校准置信度达到 0.80 时，Drafter 跳过本轮根请求，仍可根据该动作的结果预测下一步。

环境契约支持 `exact`、`allowlist`、`denylist`、`isolated`。投影列表由 `environmentKeys` 提供，隔离环境由 `isolatedEnvironment` 提供。默认比较完整业务环境；`PI_SUBCOMPUTE_SOCKET`、`PI_SUBCOMPUTE_CONTEXT` 两个协议句柄排除在环境摘要之外，受控通道读取的输入纳入进程证书。`replayAcceptedTaints` 默认空列表；用户可显式允许负载所需的 `clock`、`random`、`pid_observation`、`descriptor_observation` 等标记。

Linux 进程证据按执行域记录未知系统调用，并标记实际调用它的进程映像。未知调用、`MADV_REMOVE`，以及对共享文件映射请求写权限的 `mprotect` 或 `pkey_mprotect` 会产生 `unsupported_syscall`。对继承文件描述符的写操作若无法定位到文件路径，证书会记录 `unresolved_fd_write`，由原生进程执行该命令。

运行时服务共享随设置更新的配置引用，后台回收使用当前策略。ext4、XFS、Btrfs、F2FS 默认使用 100 ms 文件静置窗，其他文件系统保留 2000 ms 默认值。Windows 对象租约助手默认空闲保活 30 秒，可通过 `filesystem.objectLeaseIdleMs` 调整；解释器启动失败进入重试冷却期，文件工具继续正常执行。

`nativeBuild` 容量用于编译 Linux 助手。调整后将同一份 `config` 对象保存为 JSON，执行 `npm run setup:linux -- --config /path/to/config.json`，再重启执行域。运行时核对源码和编译配置指纹。

## 外部程序协议

在 Host 管理的工具调用中调用 `await subcomputeEnvironment()`，把返回的 `PI_SUBCOMPUTE_SOCKET` 与 `PI_SUBCOMPUTE_CONTEXT` 传给子进程。Windows 使用 Named Pipe，Linux 使用 Unix socket。设置 `subcomputation.exposeToControlledProcesses: true` 可向受控进程自动注入这些变量。

Linux 受控 Bash 默认共享 CPython 导入源码时的编译结果。Actor 和各推测来源在同一 Host 内都能产生和使用这些结果。Python 已有有效 `.pyc` 时仍优先使用它。需要编译时，缓存按解释器身份、源文件名、优化级别和源码 SHA-256 匹配。命中后模块顶层代码仍在调用方执行。状态报告的 `python.source_to_code` 记录实际命中次数和复用的编译耗时。可用 `subcomputation.pythonImports: false` 关闭该接入。

每个连接使用换行分隔的 JSON，可在同一调用令牌有效期内顺序执行多个步骤，共享输入会话。每个步骤执行一次查询，未命中时再发布结果：

```json
{"op":"compute","context":"调用令牌","namespace":"example.transform","key":"job","inputResources":[{"path":"input.bin","scope":"content"}],"envKeys":["MODE"],"environment":{"MODE":"fast"}}
```

命中返回 `state: "hit"` 和 `value`。未命中返回 `state: "miss"` 、步骤令牌 `token` 及输入快照的路径和元数据。`includeInputContent` 默认 `false`；需要响应携带文件内容时，在查询中显式设置 `includeInputContent: true`，内容以 Base64 返回。外部程序发现新的输入时，先发送观测请求。收到 `{"state":"observed","token":"步骤令牌"}` 后读取该输入。计算完成后用同一个令牌发布结果。

```json
{"op":"observe","token":"步骤令牌","reads":[{"path":"include/config.h","scope":"content"}]}
{"op":"publish","token":"步骤令牌","value":{"encoding":"base64","value":"YWJj"},"durationMs":12.5}
```

发布帧也可用 `reads` 声明最后一批依赖。发布时会重新验证所有已观测输入，同一步内资源发生变化会返回错误。后续相同查询会验证已保存的动态依赖。

结构化输出使用 `{"encoding":"json","value":...}`。发布完成返回 `state: "published"`；错误返回 `state: "error"`。令牌随工具调用结束或超时撤销。示例见 [JavaScript 客户端](test/subcompute-client.mjs)和 [Python 客户端](test/subcompute-client.py)。

## Linux 进程复用

```sh
npm run setup:linux
sudo ~/.local/bin/pi-speculative-domain --user "$(id -u)" env PATH="$PATH" "$(command -v pi)"
```

执行域需要 cgroup v2、内核 BTF、eBPF tracing/kprobe hooks 和 `bpf_loop`。构建依赖 Clang、libbpf、libelf、zlib 及安装脚本报告的工具。使用 Linux 原生文件系统上的 checkout。

Linux 后端把工具与 Bash 实际读取的工作区文件按路径和内容摘要存入共享输入库。后续调用验证文件版本后可借用同一份不可变内容。真实 Actor 还会为已退出、输出完整且输入可验证的只读子进程发布证书，其他 Bash 命令可复用其中相同的子进程。文件输入的成功发布与复用分别计入 `fileInputsPublished` 和 `fileInputHits`。

`actorObservationMode: "provenance"` 为真实 Actor 执行采集依赖；当前 Actor 沉淀支持可完整验证的只读命令。写入、外部状态或不完整观测会阻止该次进程证书发布。独立文件输入按各自的版本验证。编辑后主动重跑旧命令的 `prepareObservedOperations` 默认关闭。

Actor 观测与普通受控执行共享子进程采用通道。纯字节输出的子进程可以多次重放，环境和非确定性接纳范围由显式契约控制。内核观测容量位于 `nativeBuild`；观测历史丢失时诊断报告 `observer_history_incomplete` 及原因位。

标准输出能否跨 pipe/socket 载体复用，由真实 syscall 观测决定。程序探测标准流元数据时，证书保留载体约束。受控异步启动使用 `createControlledBashOperations`。子进程观察另需合格的 `PI_SPEC_HELD_EXEC_OBSERVER`，安装脚本和环境诊断会报告当前资格。

进程中途续接、OFD 图迁移和可恢复进程镜像已移除。

## Windows 文件资源复用

Windows 使用显式子计算协议及文件资源复用。文件租约由 Python 3 标准库 `ctypes` 调用 `OpenFileById` 持有，支持文件和父目录重命名、删除后身份校验及硬链接。运行环境需要可执行的 Python 3，默认命令为 `python`；可通过 `filesystem.windowsObjectLeaseCommand` 指定，例如 `["py", "-3"]` 或 Python 的绝对路径。

辅助进程按解释器命令共享；容量热更新沿用同一进程，最后一个租约释放后按 `filesystem.objectLeaseIdleMs` 等待退出。GC、工作线程退出和父进程管道关闭会回收句柄。`filesystem.objectLeaseTimeoutMs` 控制请求超时，`filesystem.maxWindowsLeaseRequests` 控制请求并发；批量释放会等待可用容量。实现随包附带，原有 `.node` 模块及其编译步骤已移除。

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
