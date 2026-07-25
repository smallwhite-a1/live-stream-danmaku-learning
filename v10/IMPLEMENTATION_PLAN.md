# V10 实现与验证清单

这份清单记录 V10 实际已经完成的工作。只有在本机得到直接证据的项目才标记为完成。

验证日期：2026-07-26。

---

## 1. 页面与状态层

- [x] 建立 React、TypeScript、Vite 项目
- [x] 建立 `/`、`/monitor`、`/chain` 三个前端路由
- [x] 从 URL、本地存储和默认值解析用户身份
- [x] 对身份字段执行空值和 64 字符上限校验
- [x] 实现 `101`、`102`、`103`、`104` 四种协议
- [x] 对服务端 JSON 做运行时结构校验
- [x] 实现 WebSocket 建连、收包、发包和手动重连
- [x] 实现 500ms 到 10s 的指数退避
- [x] 使用连接代次阻止旧连接回调污染新连接
- [x] 切换房间时清理旧连接、消息、统计和重试状态
- [x] 弹幕与点赞使用独立限流倒计时

## 2. 直播间

- [x] 实现本地图片直播舞台
- [x] 实现桌面 8 轨、手机 4 轨弹幕
- [x] 实现轨道最早可用时间分配
- [x] 保证弹幕轨道位于房间标题下方
- [x] 限制活动弹幕为 40 条
- [x] 限制消息列表为 300 条
- [x] 展示在线人数、点赞、连接状态和本次消息数
- [x] 展示实时消息列表
- [x] 实现弹幕输入、发送与点赞
- [x] 将控制消息转换为中文操作反馈

## 3. 运行监控

- [x] 每 2 秒请求一次 `/metrics`
- [x] 阻止上一轮指标请求未结束时重复发起
- [x] 页面卸载时取消指标请求
- [x] 保留最近 30 个指标样本
- [x] 使用累计值差分计算每秒速率
- [x] 计数器重置时避免负速率
- [x] 保留最近 50 个治理事件
- [x] 展示当前连接、投递速率、入口丢弃累计和限流累计
- [x] 区分发送队列丢弃与慢客户端断开
- [x] 展示 goroutine 和内存
- [x] 展示 Redis 与 Kafka Producer 的真实状态
- [x] stale 时将 Redis、Kafka 标记为数据过期
- [x] 将 Consumer 与 MySQL 明确标记为当前接口不可观测
- [x] 为趋势图提供等价的无障碍数据表

## 4. 链路页

- [x] 展示浏览器、WebSocket、Manager、Redis、Kafka、Consumer、MySQL
- [x] 将实时广播与异步持久化画成两条分支
- [x] 不根据 Kafka Producer 状态推断 Consumer 状态
- [x] 不根据 Server 指标推断 MySQL 健康
- [x] 将 V11 AI 消费者放在 Kafka 后的独立旁路
- [x] 验证 AI 分支不属于实时广播和当前 Consumer 路径

## 5. Go 静态前端托管

- [x] Go Server 支持 `-web-dir`
- [x] 正常返回构建后的静态资源
- [x] `/monitor` 和 `/chain` 深层路由回退到 `index.html`
- [x] 阻止静态文件路径逃逸根目录
- [x] `/ws`、`/metrics`、`/health` 继续由 Go 处理

## 6. 自动化测试

- [x] 前端协议测试
- [x] 身份解析测试
- [x] WebSocket 生命周期和重连测试
- [x] 弹幕轨道与资源上限测试
- [x] 发送区与限流反馈测试
- [x] 指标差分和 stale 测试
- [x] 监控页依赖状态测试
- [x] 链路分支归属测试
- [x] 真实双浏览器广播与点赞测试
- [x] 真实服务端限流与恢复测试
- [x] 真实房间切换隔离测试
- [x] 浏览器真实布局测试锁定弹幕不遮挡标题

## 7. 最终验证命令

### 7.1 后端

- [x] `gofmt -w $(rg --files v10 -g '*.go')`
- [x] `go test -count=1 ./v10/...`
- [x] `go test -race -count=1 ./v10/internal/...`
- [x] `go vet ./v10/...`
- [x] `docker compose -f v10/docker-compose.redis-kafka-mysql.yaml config --quiet`

结果：

```text
Go 包测试通过
竞态检测通过
静态分析通过
Compose 配置解析通过
```

### 7.2 前端

- [x] `npm test`
- [x] `npm run lint`
- [x] `npm run build`
- [x] `npm run test:e2e`

结果：

```text
11 个前端测试文件通过
113 项前端测试通过
3 项真实浏览器联调通过
生产构建通过
```

### 7.3 Go 托管生产构建

- [x] 启动 `go run ./v10/cmd/server -port=18081 -redis=false -kafka=false -web-dir=./v10/web/dist`
- [x] `GET /health` 返回 `ok`
- [x] `GET /monitor` 返回单页应用
- [x] `GET /metrics` 返回 JSON

### 7.4 响应式浏览器检查

- [x] `1440 x 900`
- [x] `1024 x 768`
- [x] `390 x 844`
- [x] 无横向溢出
- [x] 无控件重叠
- [x] 弹幕位于房间标题下方
- [x] 手机发送区可到达
- [x] 手机监控卡片换行
- [x] 趋势图非空
- [x] 本地舞台图片加载成功

## 8. Docker 中间件验收

- [x] `docker info`
- [x] Docker Server 29.6.1 可用
- [x] `redis:7` 镜像下载成功
- [ ] 完整 Compose 服务启动
- [ ] 执行 V10 数据库迁移
- [ ] 启动真实 Consumer
- [ ] 验证 Redis 跨实例广播
- [ ] 验证 Kafka ACK
- [ ] 验证 MySQL 行数据
- [ ] 验证 Redis 故障与恢复
- [ ] 验证 MySQL 暂停与恢复

阻塞证据：

```text
Docker Desktop 的阿里云镜像代理拉取 bitnami/kafka:3.7 时返回 403 Forbidden：
failed to resolve reference "docker.io/bitnami/kafka:3.7"
```

这表示 Docker 引擎已经恢复可用，但当前 Compose 的 Kafka 镜像没有在本机完成拉取。完整中间件结果不得标记为通过。

## 9. 交付边界

- [x] V1 到 V9 未修改
- [x] `node_modules`、`dist`、测试结果和浏览器报告已忽略
- [x] 无中间件模式完成真实闭环
- [x] V10 学习文档说明可观测性边界
- [x] V11 AI 仅作为异步扩展设计，没有进入实时热路径
- [ ] 修复 Kafka 镜像来源后重新执行完整中间件验收
