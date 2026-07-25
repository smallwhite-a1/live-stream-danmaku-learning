# Live Stream Danmaku 学习迭代路线

这个仓库将一个完整直播弹幕项目拆成 V1 到 V10。每一步只引入当前阶段真正需要解决的问题，适合已经掌握 Go 基础语法、正在学习并发和后端工程的同学。

## 仓库关系

- 原项目：[charlesAcmen/live-stream-danmaku](https://github.com/charlesAcmen/live-stream-danmaku)
- 学习仓库：[smallwhite-a1/live-stream-danmaku-learning](https://github.com/smallwhite-a1/live-stream-danmaku-learning)
- 根目录 `cmd/`、`internal/` 和原 README：保留原项目作为最终参考
- `v1/` 到 `v10/`：按学习顺序重新实现和解释
- `main`：指向当前完整学习版本 V10

原 README 中的性能数字来自原作者环境。学习版本没有复用这些数字冒充本机测试；V10 的本机验证命令和边界写在 [V10 README](v10/README.md)。

## 分支导航

| 分支 | 本阶段主题 | 主要学习问题 | 文档 |
|---|---|---|---|
| `v1` | 单机 WebSocket 最小闭环 | 一个连接怎样读写，房间怎样广播 | [进入 V1](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v1/v1) |
| `v2` | Worker Pool 广播 | 为什么不能为每次广播无限创建 goroutine | [进入 V2](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v2/v2) |
| `v3` | 单机广播性能优化 | 房间快照、锁粒度、对象复用与慢客户端 | [进入 V3](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v3/v3) |
| `v4` | MySQL 持久化 | GORM、仓储层、同步落库的代价 | [进入 V4](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v4/v4) |
| `v5` | Kafka 异步持久化 | 实时广播与落库怎样解耦 | [进入 V5](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v5/v5) |
| `v6` | Redis 跨实例广播 | 多个 Server 怎样共享房间消息 | [进入 V6](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v6/v6) |
| `v7` | 在线人数与点赞 | 高频计数怎样聚合并定时广播 | [进入 V7](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v7/v7) |
| `v8` | Kafka 到 MySQL 可靠落库 | 幂等、位点、批量写入、死信和恢复 | [进入 V8](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v8/v8) |
| `v9` | 流量治理与故障隔离 | 限流、熔断、降级、背压和分区恢复 | [进入 V9](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v9/v9) |
| `v10` | Web 可视化与真实联调 | 浏览器状态、重连、监控、链路展示与端到端测试 | [进入 V10](https://github.com/smallwhite-a1/live-stream-danmaku-learning/tree/v10/v10) |

## 分支组织方式

分支采用累积式历史：

```text
原项目快照
  -> v1 提交
  -> v2 提交
  -> v3 提交
  -> ...
  -> v9 提交
  -> V10 的多次前端与测试提交
  -> main
```

因此：

- `v1` 只包含原项目参考和 V1；
- `v5` 包含 V1 到 V5；
- `v10` 包含完整 V1 到 V10；
- 每个分支都能独立阅读，不依赖未来版本；
- Git 提交历史能看到功能为什么逐步出现。

## 推荐阅读方式

第一次学习：

```bash
git switch v1
```

完成当前阶段后切换下一分支：

```bash
git switch v2
```

查看两个阶段新增了什么：

```bash
git diff --stat v7..v8
git diff v7..v8 -- v8
```

查看某一阶段的完整提交：

```bash
git log --oneline --decorate --graph v10
```

## 学习重点

### V1-V3：先把 Go 并发讲清楚

重点理解：

- goroutine 的创建和退出；
- channel 的生产者、消费者和关闭责任；
- `mutex`、`RWMutex` 的锁范围；
- worker pool 为什么要有固定并发；
- `safeSend` 为什么必须非阻塞；
- 慢客户端为什么不能拖住整个房间。

### V4-V7：再理解中间件如何改变链路

重点理解：

- 同步 MySQL 为什么进入实时热路径；
- Kafka 怎样把实时广播和落库解耦；
- Redis Pub/Sub 怎样支持跨实例房间广播；
- 点赞为什么先在内存聚合，再批量写 Redis；
- 定时统计 goroutine 怎样避免重入。

### V8-V9：进入工程可靠性

重点理解：

- MySQL 成功后再标记 Kafka 位点；
- `message_id` 唯一索引怎样吸收重复消费；
- 坏消息与依赖故障为什么要分开处理；
- 限流保护的是哪一层资源；
- Redis 故障时为什么允许本机降级；
- MySQL 故障为什么暂停当前 Kafka 分区。

### V10：把系统变成可操作产品

重点理解：

- 浏览器 WebSocket 连接生命周期；
- 指数退避和旧连接代次；
- 前端消息、弹幕和图表为什么也要有界；
- 累计指标如何差分成每秒速率；
- 监控为什么必须区分正常、过期、未启用和不可观测；
- AI 为什么应从 Kafka 异步旁路接入，而不是进入实时广播热路径。

## 当前验证边界

V10 已在本机验证：

- Go 测试、竞态检测和静态分析；
- 113 项前端单元与组件测试；
- 3 项真实浏览器与 Go 服务联调；
- Go 直接托管构建后的前端；
- 桌面、中屏和手机响应式布局；
- 无 Redis、Kafka、MySQL 时的本机广播闭环。

完整 Docker 中间件链路尚未在本机完成：Docker 引擎已经可用，但当前镜像代理拉取 `bitnami/kafka:3.7` 返回 `403 Forbidden`。这项边界在 V10 文档中保留为未验证，而不是写成通过。
