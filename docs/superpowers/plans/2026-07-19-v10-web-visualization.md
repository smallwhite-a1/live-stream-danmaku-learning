# V10 Web Visualization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变 V9 实时链路语义的前提下，新增可发送和展示弹幕、查看真实运行指标并讲解完整处理链路的 V10 Web 可视化版本。

**Architecture:** V10 从 V9 复制为独立学习版本，Go 服务继续负责 WebSocket、指标和中间件链路，并增加可选静态文件托管。React 前端通过同源 `/ws`、`/metrics` 和 `/health` 接入；开发模式使用 Vite 代理，构建后由 Go 服务托管。

**Tech Stack:** Go 1.25、React、TypeScript、Vite、React Router、Lucide、Recharts、Vitest、React Testing Library、Playwright

## Global Constraints

- 不修改 `v9/` 中的任何文件。
- 新版本必须位于 `v10/`，能够独立运行和测试。
- WebSocket 协议继续使用 `101`、`102`、`103`、`104` 四种消息类型。
- 前端不得同步调用 Redis、Kafka、MySQL 或任何 AI 服务。
- 无中间件模式必须完成双浏览器弹幕和点赞闭环。
- Redis 和 Kafka 状态必须来自 `/metrics`；MySQL 显示“当前接口不可观测”。
- 聊天消息最多保留 300 条，活跃飘屏最多 40 条，指标样本最多 30 个，治理事件最多 50 条。
- 指标每 2 秒采样一次；失败时保留最后快照并标记 `stale`。
- WebSocket 重连从 500ms 开始，指数增长，上限 10s。
- 桌面最多 8 条弹幕轨道，手机最多 4 条。
- UI 使用已确认的“信号控制台”风格，不使用营销落地页、装饰性渐变或伪造监控数据。
- Docker 测试仅在引擎实际可用时执行；不可用时必须明确记录未验证范围。

---

## File Map

### Go 基线与静态托管

- `v10/cmd/server/main.go`: V10 服务入口、现有 API 路由、可选静态目录。
- `v10/internal/webapp/handler.go`: 静态资源与 SPA fallback。
- `v10/internal/webapp/handler_test.go`: 静态托管行为测试。
- `v10/**`: 从 V9 复制并机械更新包路径、环境变量、资源名称和端口。

### 前端协议与状态

- `v10/web/src/protocol/types.ts`: WebSocket 与指标类型。
- `v10/web/src/protocol/parser.ts`: 服务端消息解析和客户端消息编码。
- `v10/web/src/realtime/useDanmakuSocket.ts`: 连接、重连、消息和控制状态。
- `v10/web/src/metrics/derive.ts`: 累计指标差分和治理事件推导。
- `v10/web/src/metrics/useMetrics.ts`: 指标轮询、过期状态和采样历史。

### 页面与组件

- `v10/web/src/app/App.tsx`: 路由外壳、用户和房间状态。
- `v10/web/src/pages/LiveRoomPage.tsx`: 直播间。
- `v10/web/src/pages/MonitorPage.tsx`: 运行监控。
- `v10/web/src/pages/ChainPage.tsx`: 链路说明。
- `v10/web/src/components/*`: 顶栏、舞台、消息、输入、指标、依赖状态。
- `v10/web/src/styles/*`: 设计变量、全局布局和响应式样式。
- `v10/web/src/assets/live-stage.webp`: 本地演示画面。

### 验证与文档

- `v10/web/src/**/*.test.ts(x)`: 前端单元和组件测试。
- `v10/web/e2e/live-room.spec.ts`: 双浏览器功能测试。
- `v10/web/playwright.config.ts`: E2E 服务编排。
- `v10/README.md`: V10 学习文档。

---

### Task 1: Create the Independent V10 Go Baseline

**Files:**
- Create: `v10/cmd/benchmark/main.go`
- Create: `v10/cmd/client/main.go`
- Create: `v10/cmd/consumer/main.go`
- Create: `v10/cmd/migrate/main.go`
- Create: `v10/cmd/query/main.go`
- Create: `v10/cmd/server/main.go`
- Create: `v10/internal/consumer/handler.go`
- Create: `v10/internal/consumer/handler_test.go`
- Create: `v10/internal/idgen/generator.go`
- Create: `v10/internal/idgen/generator_test.go`
- Create: `v10/internal/infra/db.go`
- Create: `v10/internal/infra/kafka.go`
- Create: `v10/internal/infra/kafka_test.go`
- Create: `v10/internal/infra/redis.go`
- Create: `v10/internal/model/message.go`
- Create: `v10/internal/queue/kafka_dead_letter.go`
- Create: `v10/internal/queue/kafka_publisher.go`
- Create: `v10/internal/queue/kafka_publisher_test.go`
- Create: `v10/internal/queue/publisher_health.go`
- Create: `v10/internal/ratelimit/controller.go`
- Create: `v10/internal/ratelimit/controller_test.go`
- Create: `v10/internal/repo/message_repo.go`
- Create: `v10/internal/resilience/breaker.go`
- Create: `v10/internal/resilience/breaker_test.go`
- Create: `v10/internal/ws/client.go`
- Create: `v10/internal/ws/handler.go`
- Create: `v10/internal/ws/manager.go`
- Create: `v10/internal/ws/manager_test.go`
- Create: `v10/docker-compose.redis-kafka-mysql.yaml`

**Interfaces:**
- Consumes: V9 source tree as a read-only baseline.
- Produces: A standalone V10 Go backend with the same behavior and V10-specific resource names.

- [ ] **Step 1: Copy the V9 source tree**

Run:

```bash
cp -R v9 v10
rm v10/IMPLEMENTATION_PLAN.md
rm v10/README.md
```

Expected: `v10/` contains only the Go source, tests, and Compose file copied from V9.

- [ ] **Step 2: Mechanically update versioned identifiers**

Run one mechanical rewrite over text files:

```bash
rg -l 'livestream-danmaku/v9|V9|v9[_:]|danmaku_v9|6383|3312|9098' v10 \
  | xargs perl -pi -e '
    s{github\.com/charlesAcmen/livestream-danmaku/v9}{github.com/charlesAcmen/livestream-danmaku/v10}g;
    s/V9_/V10_/g;
    s/\bV9\b/V10/g;
    s/v9_danmaku_save_topic/v10_danmaku_save_topic/g;
    s/v9_danmaku_save_dlq/v10_danmaku_save_dlq/g;
    s/v9_danmaku_save_group/v10_danmaku_save_group/g;
    s/v9_danmaku_messages/v10_danmaku_messages/g;
    s/uk_v9_message_id/uk_v10_message_id/g;
    s/idx_v9_room_cursor/idx_v10_room_cursor/g;
    s/danmaku_v9/danmaku_v10/g;
    s/"v9:/"v10:/g;
    s/6383/6384/g;
    s/3312/3313/g;
    s/9098/9099/g;
  '
```

The replacements produce:

```text
github.com/charlesAcmen/livestream-danmaku/v9
-> github.com/charlesAcmen/livestream-danmaku/v10

V9_
-> V10_

v9_danmaku_save_topic
-> v10_danmaku_save_topic

v9_danmaku_save_dlq
-> v10_danmaku_save_dlq

v9_danmaku_save_group
-> v10_danmaku_save_group

v9_danmaku_messages
-> v10_danmaku_messages

uk_v9_message_id
-> uk_v10_message_id

idx_v9_room_cursor
-> idx_v10_room_cursor

danmaku_v9
-> danmaku_v10
```

Compose and Go defaults must then contain:

```text
Redis: 6384 -> 6379
MySQL: 3313 -> 3306
Kafka: 9099 -> 9094
```

- [ ] **Step 3: Verify no V9 identifiers remain**

Run:

```bash
rg -n 'livestream-danmaku/v9|\bV9\b|V9_|v9[_:]|danmaku_v9|6383|3312|9098' v10
```

Expected: no output.

- [ ] **Step 4: Format and run the copied test suite**

Run:

```bash
gofmt -w $(rg --files v10 -g '*.go')
go test -count=1 ./v10/...
go test -race -count=1 ./v10/internal/...
go vet ./v10/...
```

Expected: all commands exit with status 0.

- [ ] **Step 5: Commit the standalone baseline**

```bash
git add v10
git commit -m "feat: create v10 backend baseline"
```

---

### Task 2: Add Optional Go Static Hosting

**Files:**
- Create: `v10/internal/webapp/handler.go`
- Create: `v10/internal/webapp/handler_test.go`
- Modify: `v10/cmd/server/main.go`

**Interfaces:**
- Consumes: A directory containing `index.html` and Vite assets.
- Produces: `webapp.NewHandler(root string) (http.Handler, error)`.

- [ ] **Step 1: Write failing static-handler tests**

Create tests with these cases:

```go
func TestNewHandlerRequiresIndex(t *testing.T)
func TestHandlerServesExistingAsset(t *testing.T)
func TestHandlerFallsBackToIndexForClientRoute(t *testing.T)
func TestHandlerDoesNotTreatDirectoryAsAsset(t *testing.T)
func TestHandlerRejectsUnsupportedMethod(t *testing.T)
func TestHandlerCannotEscapeRoot(t *testing.T)
```

Representative assertion:

```go
handler, err := NewHandler(root)
if err != nil {
    t.Fatal(err)
}
request := httptest.NewRequest(http.MethodGet, "/monitor", nil)
response := httptest.NewRecorder()
handler.ServeHTTP(response, request)
if response.Code != http.StatusOK {
    t.Fatalf("status=%d", response.Code)
}
if !strings.Contains(response.Body.String(), `<div id="root"></div>`) {
    t.Fatalf("expected SPA index, body=%q", response.Body.String())
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./v10/internal/webapp -run Test -count=1
```

Expected: FAIL because `NewHandler` does not exist.

- [ ] **Step 3: Implement the SPA handler**

Implement this public contract:

```go
package webapp

func NewHandler(root string) (http.Handler, error)
```

Behavior:

```text
1. root/index.html must exist and be a regular file.
2. A requested regular file is served directly.
3. A missing path or directory path serves index.html.
4. filepath.Clean and filepath.Rel prevent leaving root.
5. GET and HEAD are accepted; other methods return 405.
```

Use `http.ServeFile` for both assets and the fallback.

- [ ] **Step 4: Wire `-web-dir` into the server**

Add:

```go
webDir := flag.String(
    "web-dir",
    getenv("V10_WEB_DIR", ""),
    "optional directory containing the built web application",
)
```

After registering `/ws`, `/health`, and `/metrics`:

```go
if *webDir != "" {
    webHandler, err := webapp.NewHandler(*webDir)
    if err != nil {
        log.Printf("[server] web frontend unavailable dir=%s err=%v", *webDir, err)
    } else {
        mux.Handle("/", webHandler)
        log.Printf("[server] web frontend enabled dir=%s", *webDir)
    }
}
```

The server must continue starting when the directory is missing.

- [ ] **Step 5: Run focused and full Go verification**

Run:

```bash
go test -count=1 ./v10/internal/webapp ./v10/cmd/server
go test -count=1 ./v10/...
go test -race -count=1 ./v10/internal/...
```

Expected: all tests pass.

- [ ] **Step 6: Commit static hosting**

```bash
git add v10/internal/webapp v10/cmd/server/main.go
git commit -m "feat: serve v10 web application"
```

---

### Task 3: Scaffold the Frontend and Implement the Protocol Layer

**Files:**
- Create: `v10/web/package.json`
- Create: `v10/web/package-lock.json`
- Create: `v10/web/index.html`
- Create: `v10/web/tsconfig.json`
- Create: `v10/web/tsconfig.app.json`
- Create: `v10/web/tsconfig.node.json`
- Create: `v10/web/vite.config.ts`
- Create: `v10/web/src/main.tsx`
- Create: `v10/web/src/protocol/types.ts`
- Create: `v10/web/src/protocol/parser.ts`
- Create: `v10/web/src/protocol/parser.test.ts`
- Create: `v10/web/src/test/setup.ts`

**Interfaces:**
- Consumes: Raw WebSocket strings and the V10 packet schema.
- Produces: `parseServerPacket`, `encodeDanmaku`, `encodeLike`, and shared TypeScript types.

- [ ] **Step 1: Create the Vite TypeScript project and install dependencies**

Run from `v10/`:

```bash
npm create vite@latest web -- --template react-ts
cd web
npm install
npm install react-router-dom lucide-react recharts
npm install -D vitest jsdom @types/node @testing-library/react @testing-library/jest-dom @testing-library/user-event
rm -f src/App.css src/index.css src/assets/react.svg public/vite.svg
```

Add package scripts:

```json
{
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "eslint ."
  }
}
```

- [ ] **Step 2: Configure Vite proxy and Vitest**

`vite.config.ts` must import `defineConfig` from `vitest/config` so the test property is typed:

```ts
import { loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const backend = env.VITE_BACKEND_TARGET || "http://127.0.0.1:8080";

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/ws": { target: backend, ws: true },
        "/metrics": { target: backend },
        "/health": { target: backend },
      },
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test/setup.ts",
    },
  };
});
```

- [ ] **Step 3: Define protocol types**

Define:

```ts
export const PacketType = {
  Danmaku: 101,
  Stats: 102,
  Like: 103,
  Control: 104,
} as const;

export interface DanmakuMessage {
  id: number;
  message_id: string;
  room_id: string;
  user_id: string;
  username: string;
  content: string;
  send_time: string;
}

export interface RoomStats {
  online: number;
  likes: number;
}

export interface ControlMessage {
  code: "rate_limited" | "server_overloaded" | "content_too_long" | string;
  action?: string;
  scope?: string;
  retry_after_millis?: number;
}

export type ServerEvent =
  | { kind: "danmaku"; roomId: string; data: DanmakuMessage }
  | { kind: "stats"; roomId: string; data: RoomStats }
  | { kind: "control"; roomId: string; data: ControlMessage };
```

- [ ] **Step 4: Write failing parser tests**

Cover:

```ts
it("parses a danmaku packet")
it("parses a room stats packet")
it("parses a control packet")
it("returns null for malformed JSON")
it("returns null for an unknown packet type")
it("encodes trimmed danmaku content")
it("encodes a bounded like count")
```

Example:

```ts
expect(parseServerPacket(JSON.stringify({
  type: 102,
  room_id: "room-01",
  data: { online: 12, likes: 34 },
}))).toEqual({
  kind: "stats",
  roomId: "room-01",
  data: { online: 12, likes: 34 },
});
```

- [ ] **Step 5: Run tests and verify they fail**

Run:

```bash
npm test -- src/protocol/parser.test.ts
```

Expected: FAIL because parser functions do not exist.

- [ ] **Step 6: Implement parser and encoders**

Public functions:

```ts
export function parseServerPacket(raw: string): ServerEvent | null
export function encodeDanmaku(content: string): string
export function encodeLike(count = 1): string
```

`encodeLike` clamps count to `1..20`. `encodeDanmaku` trims content and throws when it is empty.

- [ ] **Step 7: Run frontend tests and build**

Run:

```bash
npm test
npm run build
```

Expected: parser tests pass and `dist/index.html` is generated.

- [ ] **Step 8: Commit frontend foundation**

```bash
git add v10/web
git commit -m "feat: add v10 web protocol foundation"
```

---

### Task 4: Implement WebSocket Connection and Reconnection

**Files:**
- Create: `v10/web/src/realtime/useDanmakuSocket.ts`
- Create: `v10/web/src/realtime/useDanmakuSocket.test.tsx`
- Create: `v10/web/src/realtime/url.ts`
- Create: `v10/web/src/realtime/url.test.ts`

**Interfaces:**
- Consumes: `Identity`, protocol parser, browser WebSocket.
- Produces: `useDanmakuSocket(identity)` with status, messages, stats, control state, and send operations.

- [ ] **Step 1: Define the hook interface**

```ts
export interface Identity {
  userId: string;
  username: string;
  roomId: string;
}

export type SocketStatus =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "offline";

export interface DanmakuSocketState {
  status: SocketStatus;
  messages: DanmakuMessage[];
  stats: RoomStats;
  lastControl: ControlMessage | null;
  retryUntil: number;
  sendDanmaku(content: string): boolean;
  sendLike(count?: number): boolean;
  reconnect(): void;
}
```

- [ ] **Step 2: Write failing URL tests**

Required behavior:

```ts
expect(buildSocketURL(
  new URL("http://localhost:5173/"),
  { userId: "u 1", username: "林", roomId: "room/1" },
)).toBe("ws://localhost:5173/ws?uid=u+1&name=%E6%9E%97&room=room%2F1");
```

HTTPS must produce `wss:`.

- [ ] **Step 3: Implement `buildSocketURL`**

```ts
export function buildSocketURL(location: URL, identity: Identity): string
```

Use `URL` and `URLSearchParams`; do not manually concatenate unescaped query values.

- [ ] **Step 4: Write failing hook tests**

Use a controllable `MockWebSocket` and fake timers. Cover:

```text
connects with encoded identity
stores incoming danmaku and stats
keeps only the newest 300 messages
sends type 101 and type 103 packets
does not report success while socket is not open
reconnects after 500ms and increases delay
caps reconnect delay at 10s
does not reconnect after component unmount
closes the old socket when identity changes
maps control retry time to retryUntil
```

- [ ] **Step 5: Run hook tests and verify they fail**

Run:

```bash
npm test -- src/realtime
```

Expected: FAIL because the hook is not implemented.

- [ ] **Step 6: Implement the hook**

Implementation rules:

```text
1. Keep the active socket, reconnect timer, attempt number, and generation in refs.
2. Ignore callbacks from sockets whose generation is stale.
3. On unexpected close, set reconnecting and schedule min(500*2^attempt, 10000).
4. On successful open, reset attempt to zero.
5. On identity change, close the old socket with an intentional-close flag.
6. Append only parsed danmaku; update stats independently.
7. Do not optimistically append the sender's own message.
8. sendDanmaku and sendLike return false unless readyState is OPEN.
```

- [ ] **Step 7: Run tests and build**

Run:

```bash
npm test
npm run build
```

Expected: all tests and TypeScript build pass.

- [ ] **Step 8: Commit realtime state**

```bash
git add v10/web/src/realtime
git commit -m "feat: manage web socket lifecycle"
```

---

### Task 5: Implement Metrics Sampling and Governance Events

**Files:**
- Create: `v10/web/src/metrics/types.ts`
- Create: `v10/web/src/metrics/derive.ts`
- Create: `v10/web/src/metrics/derive.test.ts`
- Create: `v10/web/src/metrics/useMetrics.ts`
- Create: `v10/web/src/metrics/useMetrics.test.tsx`

**Interfaces:**
- Consumes: JSON from `/metrics`.
- Produces: `deriveSample`, `deriveEvents`, and `useMetrics`.

- [ ] **Step 1: Define the metric types used by the UI**

Include exact selected fields:

```ts
export interface ServerMetrics {
  websocket: {
    rooms: number;
    clients: number;
    ingress_accepted: number;
    ingress_dropped: number;
    delivered_messages: number;
    dropped_messages: number;
    slow_client_disconnects: number;
    goroutines: number;
    alloc_bytes: number;
    traffic: {
      current_connections: number;
      danmaku_rejected_user: number;
      danmaku_rejected_room: number;
      like_rejected_user: number;
      like_rejected_room: number;
    };
    redis_circuit?: {
      state: "closed" | "open" | "half_open";
      opened: number;
      rejected: number;
      recoveries: number;
    };
  };
  kafka?: {
    enqueued: number;
    acked: number;
    dropped: number;
    errors: number;
    status: "healthy" | "degraded";
  };
  queue: { status: "healthy" | "degraded" | "disabled" | "unavailable" };
  redis: {
    status: "healthy" | "degraded" | "disabled";
    circuit: "closed" | "open" | "half_open" | "disabled";
  };
}

export interface MetricSample {
  sampledAt: number;
  raw: ServerMetrics;
  deliveredPerSecond: number;
  limitedPerSecond: number;
  droppedPerSecond: number;
}

export interface MetricEvent {
  id: string;
  code:
    | "danmaku_limited"
    | "like_limited"
    | "ingress_dropped"
    | "slow_client_disconnected"
    | "redis_state_changed"
    | "kafka_state_changed";
  level: "info" | "warning" | "error" | "recovery";
  message: string;
  delta: number;
  observedAt: number;
}
```

- [ ] **Step 2: Write failing derivation tests**

Cover:

```text
computes delivered and limited rates from cumulative values
returns zero rate for first sample
treats a counter reset as zero delta
generates events for limit increments
generates one event for Redis state transition
generates one event for Kafka state transition
does not generate events when values do not change
```

Public functions:

```ts
export function deriveSample(
  previous: MetricSample | null,
  current: ServerMetrics,
  sampledAt: number,
): MetricSample

export function deriveEvents(
  previous: ServerMetrics | null,
  current: ServerMetrics,
  sampledAt: number,
): MetricEvent[]
```

- [ ] **Step 3: Run derivation tests and verify they fail**

Run:

```bash
npm test -- src/metrics/derive.test.ts
```

Expected: FAIL because derivation functions do not exist.

- [ ] **Step 4: Implement pure metric derivation**

Rate formula:

```ts
const seconds = Math.max((sampledAt - previous.sampledAt) / 1000, 0.001);
const delta = currentValue >= previousValue
  ? currentValue - previousValue
  : 0;
const perSecond = delta / seconds;
```

Merge events from one polling cycle by event code and store the observed delta.

- [ ] **Step 5: Write failing hook tests**

Cover:

```text
polls immediately and then every 2 seconds
keeps the newest 30 samples
keeps the newest 50 governance events
sets stale after a failed refresh while preserving data
returns to fresh after the next success
aborts the active request on unmount
```

- [ ] **Step 6: Implement `useMetrics`**

Return:

```ts
export interface MetricsState {
  latest: ServerMetrics | null;
  samples: MetricSample[];
  events: MetricEvent[];
  freshness: "loading" | "fresh" | "stale";
  lastSuccessAt: number | null;
  refresh(): Promise<void>;
}
```

Use one `AbortController` per request and prevent overlapping refreshes.

- [ ] **Step 7: Run tests and build**

Run:

```bash
npm test
npm run build
```

Expected: all metrics tests and TypeScript build pass.

- [ ] **Step 8: Commit metrics state**

```bash
git add v10/web/src/metrics
git commit -m "feat: visualize runtime metric state"
```

---

### Task 6: Build the App Shell and Live Room

**Files:**
- Create: `v10/web/src/app/App.tsx`
- Create: `v10/web/src/app/App.test.tsx`
- Create: `v10/web/src/app/identity.ts`
- Create: `v10/web/src/app/identity.test.ts`
- Create: `v10/web/src/components/AppHeader.tsx`
- Create: `v10/web/src/components/ConnectionBadge.tsx`
- Create: `v10/web/src/components/DanmakuStage.tsx`
- Create: `v10/web/src/components/DanmakuStage.test.tsx`
- Create: `v10/web/src/components/MessageList.tsx`
- Create: `v10/web/src/components/MessageComposer.tsx`
- Create: `v10/web/src/components/MessageComposer.test.tsx`
- Create: `v10/web/src/pages/LiveRoomPage.tsx`
- Create: `v10/web/src/styles/tokens.css`
- Create: `v10/web/src/styles/global.css`
- Create: `v10/web/src/assets/live-stage.webp`
- Modify: `v10/web/src/main.tsx`

**Interfaces:**
- Consumes: `useDanmakuSocket`, identity storage, route state.
- Produces: The responsive live room at `/`.

- [ ] **Step 1: Write failing identity tests**

Define:

```ts
export function resolveIdentity(
  search: string,
  storage: Pick<Storage, "getItem" | "setItem">,
): Identity
```

Tests must prove:

```text
query uid/name/room override storage
stored identity is reused
missing identity creates a stable local uid
blank values fall back to room-01 and a readable nickname
```

- [ ] **Step 2: Implement identity resolution**

Store under:

```text
danmaku-lab.identity.v10
```

Generated users use:

```text
uid: web-<8 lowercase hex characters>
name: 访客-<last 4 hex characters>
room: room-01
```

- [ ] **Step 3: Write failing lane and composer tests**

For the stage, define:

```ts
export function assignLane(
  lanes: readonly number[],
  now: number,
  laneCount: number,
): number
```

It must select the earliest available lane and never exceed `laneCount - 1`.

Composer tests:

```text
trims before send
does not send blank text
shows 0/200 through 200/200
preserves content when send returns false
clears content only when send returns true
disables during retryUntil
```

Character counts must use `Array.from(content).length`, matching Go's rune-oriented limit more closely than JavaScript UTF-16 `.length`.

- [ ] **Step 4: Generate the local stage image**

Use image generation with this prompt:

```text
Wide 16:9 modern live coding broadcast studio, frontal view of a large
architectural display wall, subtle server racks and control desks, neutral
graphite environment with distinct red, green, and amber signal lights,
clean realistic editorial photography, no people, no logos, no readable
text, no gradients, designed to remain legible behind white danmaku overlays.
```

Save the selected bitmap as:

```text
v10/web/src/assets/live-stage.webp
```

- [ ] **Step 5: Implement the shell and live-room components**

Required UI:

```text
AppHeader:
  DANMAKU LAB, connection badge, user, room switch action, three route tabs

DanmakuStage:
  local image, LIVE label, room, online count, bounded moving danmaku lanes

MessageList:
  newest 300 messages, user name, send time, control notices

MessageComposer:
  200-character count, like icon button, send command button

LiveRoomPage:
  desktop stage/chat split, mobile vertical stack, four room summary metrics
```

Use Lucide icons for settings, send, heart, activity, and connection states.

The room switch interaction uses these stable accessible labels:

```text
button: 更换房间
input: 昵称
input: 房间
button: 连接房间
```

- [ ] **Step 6: Implement control-message feedback**

Exact text mapping:

```ts
const controlText = {
  rate_limited: "发送过快，请稍后重试",
  server_overloaded: "服务当前繁忙，这条弹幕没有被接收",
  content_too_long: "弹幕不能超过 200 个字符",
};
```

Unknown control codes display:

```text
服务端拒绝了本次操作
```

- [ ] **Step 7: Run component tests and build**

Run:

```bash
npm test
npm run build
```

Expected: component tests pass, image is included in a hashed Vite asset, and build exits 0.

- [ ] **Step 8: Commit the live experience**

```bash
git add v10/web/src
git commit -m "feat: build v10 live room"
```

---

### Task 7: Build Monitor and Chain Pages

**Files:**
- Create: `v10/web/src/components/MetricCard.tsx`
- Create: `v10/web/src/components/MetricChart.tsx`
- Create: `v10/web/src/components/DependencyStatus.tsx`
- Create: `v10/web/src/components/GovernanceEvents.tsx`
- Create: `v10/web/src/pages/MonitorPage.tsx`
- Create: `v10/web/src/pages/MonitorPage.test.tsx`
- Create: `v10/web/src/pages/ChainPage.tsx`
- Create: `v10/web/src/pages/ChainPage.test.tsx`
- Modify: `v10/web/src/app/App.tsx`
- Modify: `v10/web/src/styles/global.css`

**Interfaces:**
- Consumes: `MetricsState` from Task 5.
- Produces: `/monitor` and `/chain`.

- [ ] **Step 1: Write failing monitor tests**

Cover:

```text
renders current connection and delivered rate
shows disabled as 未启用
shows stale without replacing metric values with zero
shows MySQL as 当前接口不可观测
renders the newest governance event first
```

Required assertion:

```ts
expect(screen.getByText("当前接口不可观测")).toBeInTheDocument();
expect(screen.queryByText("MySQL 正常")).not.toBeInTheDocument();
```

- [ ] **Step 2: Implement monitor components**

`MonitorPage` includes:

```text
four summary cards
60-second Recharts line chart
Redis / Kafka / MySQL dependency list
go runtime metrics
newest-first governance event list
fresh / stale timestamp
```

Do not render empty chart axes before two samples exist; show:

```text
正在收集趋势数据
```

- [ ] **Step 3: Write failing chain-page tests**

Cover:

```text
renders every real processing node
marks Redis disabled when metrics say disabled
marks Kafka degraded when queue says degraded
always marks MySQL unobservable
does not contain an AI model call in the realtime branch
```

- [ ] **Step 4: Implement the chain page**

Nodes:

```text
浏览器
WebSocket 校验与限流
Manager 房间广播
Redis / 本机降级
Kafka Producer
Consumer
MySQL
```

The future AI node is visually separated after Kafka and labelled:

```text
V11 独立异步消费者
```

- [ ] **Step 5: Register routes**

Use:

```tsx
<Routes>
  <Route path="/" element={<LiveRoomPage />} />
  <Route path="/monitor" element={<MonitorPage />} />
  <Route path="/chain" element={<ChainPage />} />
  <Route path="*" element={<Navigate to="/" replace />} />
</Routes>
```

- [ ] **Step 6: Run tests, lint, and build**

Run:

```bash
npm test
npm run lint
npm run build
```

Expected: all commands pass.

- [ ] **Step 7: Commit monitoring and learning pages**

```bash
git add v10/web/src
git commit -m "feat: add runtime monitoring views"
```

---

### Task 8: Add Browser End-to-End Tests

**Files:**
- Create: `v10/web/playwright.config.ts`
- Create: `v10/web/e2e/live-room.spec.ts`
- Modify: `v10/web/package.json`
- Modify: `v10/web/package-lock.json`

**Interfaces:**
- Consumes: V10 Go server in no-middleware mode and the Vite app.
- Produces: Reproducible two-browser functional verification.

- [ ] **Step 1: Install Playwright**

Run:

```bash
cd v10/web
npm install -D @playwright/test
npx playwright install chromium
```

Add:

```json
{
  "scripts": {
    "test:e2e": "playwright test"
  }
}
```

- [ ] **Step 2: Configure two local web servers**

`playwright.config.ts`:

```ts
export default defineConfig({
  testDir: "./e2e",
  use: {
    baseURL: "http://127.0.0.1:15173",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: [
    {
      command: "go run ../cmd/server -port=18080 -redis=false -kafka=false",
      url: "http://127.0.0.1:18080/health",
      reuseExistingServer: false,
    },
    {
      command: "VITE_BACKEND_TARGET=http://127.0.0.1:18080 npm run dev -- --host 127.0.0.1 --port 15173",
      url: "http://127.0.0.1:15173",
      reuseExistingServer: false,
    },
  ],
});
```

- [ ] **Step 3: Write the two-browser test**

Create two isolated browser contexts:

```ts
const a = await browser.newContext();
const b = await browser.newContext();
const pageA = await a.newPage();
const pageB = await b.newPage();

await pageA.goto("/?uid=e2e-a&name=甲&room=e2e-room");
await pageB.goto("/?uid=e2e-b&name=乙&room=e2e-room");
```

Verify:

```text
both pages reach 已连接
A sends a unique message
A and B both display the message
B likes once
both pages eventually show a nonzero like count
monitor route loads real metrics
chain route lists all nodes
```

- [ ] **Step 4: Add rate-limit and room-switch tests**

Rate-limit test:

```text
send 12 messages without delay
expect at least one 发送过快 notice
expect input is enabled after the server retry interval
```

Room-switch test:

```text
change B to another room
A sends a unique message
B must not display it
```

- [ ] **Step 5: Run E2E and full frontend verification**

Run:

```bash
npm test
npm run lint
npm run build
npm run test:e2e
```

Expected: all commands pass.

- [ ] **Step 6: Commit E2E coverage**

```bash
git add v10/web
git commit -m "test: cover v10 browser workflow"
```

---

### Task 9: Write the Learning Guide and Perform Final Verification

**Files:**
- Create: `v10/README.md`
- Create: `v10/IMPLEMENTATION_PLAN.md`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: Completed V10 code and measured test output.
- Produces: A reproducible learning guide and honest verification record.

- [ ] **Step 1: Write the V10 README**

Required sections:

```text
1. V10 一句话定位
2. V9 与 V10 的差异
3. 前端学习前置知识
4. V10 目录结构
5. 浏览器、Go 和中间件架构图
6. 四种消息协议
7. WebSocket 重连状态机
8. 弹幕轨道和前端资源上限
9. 指标差分与 stale 设计
10. 三个页面的代码链路
11. 无中间件启动方法
12. 完整中间件启动方法
13. 测试命令和本机结果
14. Docker 未验证范围或实际结果
15. 推荐学习顺序
16. 面试高频问题
17. V11 AI 扩展接口
```

Use Mermaid for architecture and sequence diagrams.

- [ ] **Step 2: Record the completed implementation checklist**

`v10/IMPLEMENTATION_PLAN.md` contains the actual completed tasks and verified commands. Checkboxes may be marked complete only after their command has passed.

- [ ] **Step 3: Ignore generated frontend and Playwright output**

Add:

```gitignore
v10/web/node_modules/
v10/web/dist/
v10/web/test-results/
v10/web/playwright-report/
```

- [ ] **Step 4: Run final backend verification**

Run:

```bash
gofmt -w $(rg --files v10 -g '*.go')
go test -count=1 ./v10/...
go test -race -count=1 ./v10/internal/...
go vet ./v10/...
docker compose -f v10/docker-compose.redis-kafka-mysql.yaml config --quiet
```

Expected:

```text
Go commands pass.
Compose parsing passes even if the Docker engine is unavailable.
```

- [ ] **Step 5: Run final frontend verification**

Run:

```bash
cd v10/web
npm test
npm run lint
npm run build
npm run test:e2e
```

Expected: all commands pass.

- [ ] **Step 6: Verify built-static mode**

Start:

```bash
go run ./v10/cmd/server \
  -port=18081 \
  -redis=false \
  -kafka=false \
  -web-dir=./v10/web/dist
```

Verify:

```bash
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/monitor
curl -fsS http://127.0.0.1:18081/metrics
```

Expected: health is `ok`, monitor returns the SPA, metrics returns JSON.

- [ ] **Step 7: Perform responsive browser validation**

Use Playwright or the browser tool at:

```text
1440x900
1024x768
390x844
```

Capture screenshots and verify:

```text
no text overflow
no overlapping controls
danmaku stays below room header
mobile composer remains reachable
monitor cards wrap without horizontal scrolling
charts are nonblank
local stage image loads
```

- [ ] **Step 8: Attempt Docker integration only when the engine responds**

Check:

```bash
docker info
```

When it succeeds:

```bash
docker compose -f v10/docker-compose.redis-kafka-mysql.yaml up -d
go run ./v10/cmd/migrate
go run ./v10/cmd/consumer
go run ./v10/cmd/server -web-dir=./v10/web/dist
```

Then verify Redis broadcast, Kafka ACK, MySQL rows, Redis failure recovery, and MySQL pause/recovery.

When `docker info` fails, record the exact failure in README and do not claim integration success.

- [ ] **Step 9: Commit documentation and final fixes**

```bash
git add .gitignore v10
git commit -m "docs: complete v10 learning guide"
```

---

## Completion Gate

Before reporting completion:

```text
1. Review every commit for accidental V9 edits.
2. Confirm `git diff -- v9` is empty.
3. Confirm no generated node_modules, dist, screenshots, or reports are staged.
4. Confirm all claimed commands were run in the current environment.
5. Report Docker integration separately from no-middleware browser success.
```
