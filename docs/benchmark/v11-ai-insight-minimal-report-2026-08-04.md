# V11 AI 洞察最小闭环：本地实测报告

> 报告文件日期沿用实施计划约定的 `2026-08-04`；实际验证时间为 2026-08-05（Asia/Shanghai）。

## 环境

| 项目 | 实测值 |
| --- | --- |
| 操作系统 | macOS 15.6.1，Build 24G90 |
| CPU | Apple M3 |
| 架构 | `darwin/arm64` |
| Go | `go version go1.26.4 darwin/arm64` |
| Node.js | `v25.9.0` |
| npm | `11.12.1` |
| 浏览器 | Playwright Chromium 151.0.7922.34（Playwright Chromium v1234） |

没有读取、使用或持久化任何真实 API Key。

## 数据与处理结果

输入文件是 `v11/insight/testdata/fixtures/demo.jsonl`，实测 9 条 JSONL 消息：

- `room-alpha`：5 条，分布在 `12:00` 和 `12:01` 两个 60 秒窗口；
- `room-beta`：4 条，分布在 `12:00` 和 `12:01` 两个 60 秒窗口；
- 总计：4 个持久化窗口。

在真实 `insightd` 回放后的 `/history?limit=10` API 检查中，持久化结果为：`normal=4`、`degraded=0`。两个房间的最新窗口均为 `2020-01-02T12:01:00Z` 至 `2020-01-02T12:02:00Z`，规则指标都是：消息数 2、独立用户 2、问句 1、重复率 0、峰值 1 条/秒。

正常回放使用确定性的 `FakeModel`。独立 Go 集成测试将 `FakeModel.Err` 设为固定错误，确认主分析器失败后会持久化 `degraded` 结果，并保留包含该错误的降级原因、规则指标和 `rule` 模型元数据。

## 验证命令和实测时间

| 命令 | 结果 | 实测墙钟时间 |
| --- | --- | --- |
| `go test -count=1 ./...` | 9 个测试包通过；`internal/ports` 无测试文件 | 4.63 s |
| `go test -race -count=1 ./...` | 9 个测试包通过，未报告数据竞争 | 4.20 s |
| `go vet ./...` | 通过 | 0.60 s |
| `npm test` | Vitest：1 个测试文件、10 个测试通过 | 1.09 s（Vitest 自报 781 ms） |
| `npm run lint` | 通过 | 0.21 s |
| `npm run build` | 通过 | 1.26 s（Vite 构建自报 151 ms） |
| `npx playwright install chromium` | Chromium 安装成功 | 未单独计时 |
| `npm run test:e2e` | Chromium desktop 和 mobile 共 2 个测试通过 | 3.73 s（Playwright 自报 3.3 s） |

额外使用 `go test -count=1 -json ./...` 计数，得到 89 个顶层 Go 测试通过、9 个测试包通过。该计数命令用于报告，不替代上表的验收命令。

## HTTP 与浏览器实测

使用 `npm run build` 生成的 `web/dist` 和真实编译的 `insightd`，监听 `127.0.0.1:18122`：

| 检查 | 结果 |
| --- | --- |
| 服务就绪时间 | 1001 ms（进程启动至 `/health` 成功） |
| `GET /health` | `200`，响应 `{"status":"ok"}` |
| `GET .../room-alpha/.../latest` | `200`，0.000697 s，607 B |
| `GET .../room-beta/.../latest` | `200`，0.000483 s，605 B |
| 进程退出 | 发送 `SIGTERM` 后干净退出；18122 和 18123 均无遗留监听进程 |

Playwright 不使用浏览器 API mock：它先让 `insightd` 回放真实 JSONL，再提供构建后的 React 静态文件。两个 Chromium 项目均验证：

- `room-alpha` 默认加载为 `Normal`；
- 最新窗口的五项精确规则指标；
- 点击 `Neutral sentiment` 后显示 `alpha-1201-001` 证据；
- 切换到 `room-beta` 后重新展示其最新窗口；
- `document.documentElement.scrollWidth <= window.innerWidth`。

截图由成功的 E2E 测试写入：

- `.superpowers/sdd/artifacts/v11-desktop.png`：`1440x900`；
- `.superpowers/sdd/artifacts/v11-mobile.png`：`390x844`。

通过图像查看和尺寸检查确认两张图均非空：桌面图展示证据面板、稳定的五格指标和无重叠布局；移动图滚动到可读的证据面板，文字和控件未发生横向溢出。没有计算单独的像素直方图或非空像素总数。

## 发现与处理

1. 原 Task 7 brief 使用过时的 `room-001`、`room-002`，测试已使用真实 fixture ID `room-alpha`、`room-beta`。
2. brief 所说的 `Card lag` 话题不在 `room-alpha` 最新窗口：重复的卡顿消息位于较早的 `12:00` 窗口，最新 API 只返回 `12:01` 窗口。真实页面仍有带 EventID 的中性情绪可点击结论，因此没有增加会改变当前数据语义的通用模型规则，也没有引入历史窗口 UI。
3. 初次 E2E 探测确认了上述事实；随后测试改为验证真实最新窗口中的可点击证据。另修正了 iPhone 设备预设错误选择 WebKit、3x 截图比例和错误 artifact 相对路径。
4. 初次 `npm test` 发现 Vitest 会收集 `e2e/*.spec.ts`。已将 Vitest `include` 限制为 `src/**/*.test.{ts,tsx}`，使 Playwright 与 Vitest 边界清晰。

## 已知限制

- 结果、窗口和队列均为进程内内存；服务重启后会重新回放 fixture。
- 进程重启前不会淘汰已完成的内存窗口或结果；该保留与淘汰策略尚未实现。
- 单窗口超过 500 个唯一事件后，保留样本按首次到达顺序有界截取，规则详情和 `message_count` 反映的是保留事件，而不是完整窗口的精确聚合；代表性采样和保留策略是下一次迭代工作，当前未实现。
- 当前模型是 `FakeModel`，不代表真实模型的语义质量、成本、延迟或失败率。
- 本次没有测量 QPS、吞吐上限、端到端高负载延迟、真实模型质量或真实模型成本，因此不对这些指标作任何结论。
- 当前页面只读取每个房间的最新洞察，不能浏览较早窗口；这就是当前 fixture 的 `Card lag` 话题不在默认页面中的原因。
- Kafka、Redis、MySQL 和 DeepSeek 均是后续端口适配器，不在本次最小闭环中。
