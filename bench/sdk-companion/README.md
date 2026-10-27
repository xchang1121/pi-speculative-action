# 配套 SDK 的跨进程契约

这是公开 SDK 执行接缝的资格测试。`owner.mjs` 加载真实 SDK、扩展和 Host；所有模型回复由包内 faux provider 提供。`sdk-delivery.mjs` 使用公开控制器，`store.mjs` 是测试用 SQLite/CAS 适配器，仍访问现有 store 的内部接缝。它不是生产恢复 API，也不能用于任意工具或原生资源闭包。

普通 Linux/WSL 用户安装根目录锁定依赖并完成构建后运行：

```sh
npm ci --ignore-scripts
npm run check
npm run build
python3 bench/sdk-companion/run.py /absolute/new-output-directory all
python3 bench/sdk-companion/review.py /absolute/new-output-directory
```

需要 Python 3、Node、Git 和 util-linux。输出目录必须位于 checkout 之外且尚不存在。驱动用当前可用 CPU 和普通 UID 运行子进程，设置 `no_new_privs`；独立复核要求零 effective/permitted capabilities。`smoke` 只选择 baseline/normal，不能作为完整 22 场景通过。

工具效果与 SQLite 位于 Linux 临时目录，记录中的 `workDirectory` 指向保留的原始工作区；输出报告可以放在其他目录。WSL 的 `TMPDIR` 必须使用 Linux 原生文件系统。独立裸执行已复现 DrvFS 上并发 `appendFile` 丢失一条记录，不能用该日志作为两个独立工具效果的可靠计数；这项文件系统语义未计入本契约的已证明范围。

前置核验比较交付 tarball 与实际安装的全部文件，拒绝嵌套的另一份 SDK；完整源码/编译树、Node 版本和依赖锁摘要进入实现绑定。结束时核对这些输入及 Git 状态未变。输出保存阶段事件、会话日志、SQLite 状态、最终回执、自有工具效果和每个子进程终态，复核程序独立检查一次消费、原请求序号、有序结果前缀、关闭排空及真实 Host 执行。

默认序列覆盖正常执行、发行/发布/准备/claim/工具完成/最终回执/自动 append 的中断窗口、顺序与并行批次、错误与 terminate 回执、活动 owner 竞争、容量拒绝、重复接纳、关闭期间准备、监听器错误、实现变化、同 call ID 新请求及引用损坏。测试结束会终结仍存活的自有子进程，保留原始文件供审计。

证明范围是共同遵守协议、共享稳定存储路径及测试中明确绑定的自有回调。未知 committing 保留 poisoned，不能重做。未覆盖生产 store 的恢复发现与迁移、任意回调闭包、一般原生资源、外部路径替换或断电。测试用元数据表没有接入生产 GC 根协议，不能将本测试适配器直接部署为生产恢复服务。
