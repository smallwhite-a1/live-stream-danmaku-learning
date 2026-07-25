# V9 Traffic Governance and Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 在 V8 可靠落库链路上增加入口限流、Redis 故障隔离、Kafka 降级观测和 MySQL 故障暂停恢复。

**Architecture:** WebSocket 入口使用进程内令牌桶限制单用户与单房间流量，并在升级连接前执行连接准入。Redis 发布通过有界队列和固定 worker 隔离，连续失败后由状态机快速熔断并降级为本机广播；Kafka 保留非阻塞入口并报告健康状态；MySQL 失败时消费者不提交位点，按指数退避暂停当前分区，恢复后继续落库。

**Tech Stack:** Go、WebSocket、Redis、Kafka、MySQL、GORM

## Global Constraints

- V9 必须能够在 `-redis=false -kafka=false` 时独立运行。
- 实时链路不能因 Redis、Kafka 或 MySQL 故障无限阻塞。
- MySQL 未写成功时不得标记 Kafka 位点。
- 所有共享状态必须通过 `mutex` 或原子变量保护，并通过竞态检测。
- 不引入新的第三方依赖。

---

### Task 1: V9 基线

**Files:**
- Create: `v9/**`
- Modify: `v9/internal/infra/*.go`
- Modify: `v9/internal/model/message.go`

**Interfaces:**
- Produces: 独立的 V9 端口、主题、消费组、环境变量和 MySQL 表。

- [x] **Step 1:** 从 V8 复制独立目录并替换 Go 导入路径。
- [x] **Step 2:** 运行 `go test -count=1 ./v9/...`，确认行为基线通过。
- [x] **Step 3:** 将遗留的 V8 主题、表名、环境变量和容器名改为 V9。

### Task 2: 入口流量控制

**Files:**
- Create: `v9/internal/ratelimit/controller.go`
- Test: `v9/internal/ratelimit/controller_test.go`
- Modify: `v9/internal/ws/handler.go`
- Modify: `v9/internal/ws/client.go`
- Modify: `v9/internal/ws/manager.go`

**Interfaces:**
- Produces: `Controller.AcquireConnection(ip)`, `Controller.AllowDanmaku(roomID, userID)`、`Controller.AllowLike(roomID, userID)` 和 `Controller.Metrics()`。

- [x] **Step 1:** 写出连接总量、单 IP、单用户突发和单房间共享额度的失败测试。
- [x] **Step 2:** 运行 `go test ./v9/internal/ratelimit`，确认因类型尚不存在而失败。
- [x] **Step 3:** 用 `mutex` 保护计数器与令牌桶，实现最小代码。
- [x] **Step 4:** 将连接拒绝映射为 HTTP 429，将消息拒绝映射为 WebSocket 控制消息。
- [x] **Step 5:** 运行包测试与竞态检测。

### Task 3: Redis 熔断与舱壁隔离

**Files:**
- Create: `v9/internal/resilience/breaker.go`
- Test: `v9/internal/resilience/breaker_test.go`
- Modify: `v9/internal/ws/manager.go`
- Test: `v9/internal/ws/manager_test.go`

**Interfaces:**
- Produces: `Breaker.Execute(operation)`, `Breaker.Snapshot()`；Manager 内部 Redis 发布有界队列和固定 worker。

- [x] **Step 1:** 写出关闭、打开、半开探测和恢复的状态机测试。
- [x] **Step 2:** 确认测试失败后实现互斥保护的熔断器。
- [x] **Step 3:** 写出 Redis 调用失败后只做本机广播、熔断后不再调用依赖的测试。
- [x] **Step 4:** 接入 Redis 发布 worker pool、队列满降级和统计查询降级。
- [x] **Step 5:** 运行相关包测试与竞态检测。

### Task 4: 异步持久化降级与恢复

**Files:**
- Modify: `v9/internal/queue/kafka_publisher.go`
- Test: `v9/internal/queue/kafka_publisher_test.go`
- Modify: `v9/internal/consumer/handler.go`
- Test: `v9/internal/consumer/handler_test.go`

**Interfaces:**
- Produces: Kafka `healthy/degraded` 指标；MySQL `PausedPartitions`、`PauseEvents`、`Recoveries`、`RecoveryWaitMillis` 指标。

- [x] **Step 1:** 写出 Kafka 连续错误进入降级、一次成功恢复的失败测试。
- [x] **Step 2:** 实现与业务错误分离的 Kafka 健康状态记录。
- [x] **Step 3:** 写出 MySQL 首次失败、暂停、再次成功且最后才标记位点的失败测试。
- [x] **Step 4:** 实现带上限的指数退避；会话取消时停止等待且不标记位点。
- [x] **Step 5:** 运行消费者与队列包测试。

### Task 5: 配置、验证与教学文档

**Files:**
- Modify: `v9/cmd/server/main.go`
- Modify: `v9/cmd/consumer/main.go`
- Replace: `v9/README.md`

**Interfaces:**
- Produces: 可调限流阈值、Redis 熔断参数、MySQL 恢复参数和统一 JSON 指标。

- [x] **Step 1:** 接入命令行参数并在指标中展示限流、熔断和降级状态。
- [x] **Step 2:** 运行 `go test -count=1 ./v9/...`、`go test -race ./v9/internal/...` 和 `go vet ./v9/...`。
- [x] **Step 3:** 无中间件启动服务，用 20 个客户端做小规模广播冒烟测试。
- [x] **Step 4:** 用低限额制造拒绝，确认指标与客户端行为一致。
- [x] **Step 5:** 完成架构图、链路图、故障矩阵、并发设计、运行实验和面试问答文档。
