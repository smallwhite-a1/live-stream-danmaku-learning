# V11 AI Insight Minimal Closed Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone, pluggable live-room insight service that replays JSONL events, creates deterministic time windows, runs an Eino graph with a fake model, stores idempotent results in memory, and exposes them through an HTTP API and React operations page.

**Architecture:** Create a nested Go module under `v11/insight` so the AI feature has no imports from V1-V10. Domain types and ports sit at the center; JSONL, memory storage, Eino, HTTP, and React are replaceable adapters. This plan intentionally stops before DeepSeek, Kafka, Redis, and MySQL so the first deliverable is runnable and testable without external services.

**Tech Stack:** Go 1.25, Eino v0.9.13, standard `net/http`, React 19, TypeScript 6, Vite 8, Vitest, Testing Library, Playwright.

## Global Constraints

- Do not import any package below `v10/` or earlier version folders.
- The Go module path is `github.com/charlesAcmen/livestream-danmaku/v11/insight`.
- All external capabilities are injected through interfaces in `internal/ports`.
- Use 60-second event-time windows and 10-second allowed lateness by default.
- Keep at most 500 events per window and at most 8000 Unicode characters in a model prompt.
- Use bounded queues and a fixed worker pool; never launch one goroutine per window.
- Every semantic claim must reference an EventID that exists in the analyzed window.
- Fake-model and rule-fallback behavior must be deterministic across replay runs.
- No API key or real secret may be added to files, tests, logs, or Git history.
- Use TDD: each behavior starts with a failing test, followed by the smallest passing implementation.

## File Map

```text
v11/insight/
├── go.mod                         standalone Go module
├── cmd/insightd/main.go           dependency assembly and process lifecycle
├── internal/domain/
│   ├── event.go                   input event validation
│   ├── window.go                  window identity and aggregate
│   └── insight.go                 rule, semantic, and persisted result types
├── internal/ports/ports.go        source, window, analyzer, and repository interfaces
├── internal/adapters/source/jsonl/source.go
├── internal/adapters/window/memory/store.go
├── internal/adapters/repository/memory/repository.go
├── internal/adapters/analyzer/rule/analyzer.go
├── internal/adapters/analyzer/eino/
│   ├── model.go                   provider-neutral completion interface
│   ├── analyzer.go                compiled Eino graph
│   ├── prompt.go                  prompt construction and limits
│   ├── parser.go                  JSON and evidence validation
│   └── fake_model.go              deterministic model fixture
├── internal/app/
│   ├── ingest.go                  source-to-window application service
│   └── processor.go               bounded due-window processing
├── internal/httpapi/
│   ├── handler.go                 health and insight API
│   └── static.go                  optional built frontend hosting
├── testdata/fixtures/demo.jsonl   two-room, two-window deterministic dataset
├── web/                           standalone React application
└── README.md                      learning guide and verified commands
```

---

### Task 1: Standalone Domain and Ports

**Files:**
- Create: `v11/insight/go.mod`
- Create: `v11/insight/internal/domain/event.go`
- Create: `v11/insight/internal/domain/event_test.go`
- Create: `v11/insight/internal/domain/window.go`
- Create: `v11/insight/internal/domain/window_test.go`
- Create: `v11/insight/internal/domain/insight.go`
- Create: `v11/insight/internal/ports/ports.go`

**Interfaces:**
- Produces: `domain.MessageEvent`, `domain.WindowRef`, `domain.InsightWindow`, `domain.RoomInsight`.
- Produces: `ports.EventSource`, `ports.WindowStore`, `ports.InsightAnalyzer`, `ports.InsightRepository`.

- [ ] **Step 1: Create the nested module and failing event tests**

Create `go.mod`:

```go
module github.com/charlesAcmen/livestream-danmaku/v11/insight

go 1.25.0
```

Write table-driven tests asserting that blank IDs, blank room IDs, zero times, and content above 500 runes fail, while a valid Chinese event passes.

- [ ] **Step 2: Run the event tests and verify failure**

Run: `cd v11/insight && go test ./internal/domain -run TestMessageEventValidate -count=1`

Expected: FAIL because `MessageEvent` and `Validate` do not exist.

- [ ] **Step 3: Implement the event contract**

Implement:

```go
type MessageEvent struct {
    EventID       string    `json:"event_id"`
    RoomID        string    `json:"room_id"`
    UserID        string    `json:"user_id"`
    Username      string    `json:"username"`
    Content       string    `json:"content"`
    OccurredAt    time.Time `json:"occurred_at"`
    SchemaVersion string    `json:"schema_version"`
    Source        string    `json:"source"`
}

func (e MessageEvent) Validate() error
```

Validation trims identifiers, requires `EventID`, `RoomID`, `UserID`, and `OccurredAt`, accepts empty content only as an error, and limits content to 500 runes.

- [ ] **Step 4: Add failing window tests**

Test that `NewWindowRef("room-1", 12:00:59Z, time.Minute)` returns `[12:00:00Z, 12:01:00Z)`, that `Key()` is stable, and that invalid room IDs or non-positive sizes return an error.

- [ ] **Step 5: Implement window and insight types**

Define:

```go
type WindowRef struct {
    RoomID string    `json:"room_id"`
    Start  time.Time `json:"window_start"`
    End    time.Time `json:"window_end"`
}

type InsightWindow struct {
    Ref               WindowRef     `json:"ref"`
    Events            []MessageEvent `json:"events"`
    TotalMessages     int           `json:"total_messages"`
    DuplicateMessages int           `json:"duplicate_messages"`
    LateMessages      int           `json:"late_messages"`
}
```

Add `RuleStats`, `Topic`, `Sentiment`, `Question`, `Alert`, `SemanticInsight`, `ModelMeta`, `AnalysisResult`, and `RoomInsight`. `AnalysisResult` contains `Rules`, `Semantic`, and `Model`; `RoomInsight.Status` supports only `normal` and `degraded`. Add `RoomInsight.IdempotencyKey()` using room, UTC window bounds, and prompt version.

- [ ] **Step 6: Define the stable ports**

Create these exact interfaces:

```go
type AddResult struct {
    Ref       domain.WindowRef
    Added     bool
    Duplicate bool
    Late      bool
    Completed bool
}

type EventSource interface {
    Run(context.Context, func(context.Context, domain.MessageEvent) error) error
}

type WindowStore interface {
    Add(context.Context, domain.MessageEvent, time.Time) (AddResult, error)
    ClaimDue(context.Context, time.Time, int) ([]domain.WindowRef, error)
    Load(context.Context, domain.WindowRef) (domain.InsightWindow, error)
    Complete(context.Context, domain.WindowRef) error
    Release(context.Context, domain.WindowRef, time.Time) error
}

type InsightAnalyzer interface {
    Analyze(context.Context, domain.InsightWindow) (domain.AnalysisResult, error)
}

type InsightRepository interface {
    Save(context.Context, domain.RoomInsight) (bool, error)
    Latest(context.Context, string) (domain.RoomInsight, bool, error)
    List(context.Context, string, int) ([]domain.RoomInsight, error)
}
```

- [ ] **Step 7: Run and format**

Run: `cd v11/insight && gofmt -w internal/domain internal/ports && go test ./internal/domain -count=1`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add v11/insight/go.mod v11/insight/internal/domain v11/insight/internal/ports
git commit -m "feat(v11): define standalone insight domain"
```

---

### Task 2: Deterministic In-Memory Window Store

**Files:**
- Create: `v11/insight/internal/adapters/window/memory/store.go`
- Create: `v11/insight/internal/adapters/window/memory/store_test.go`

**Interfaces:**
- Consumes: `domain.MessageEvent`, `domain.WindowRef`, `ports.WindowStore`.
- Produces: `memory.New(Config) *Store` implementing `ports.WindowStore`.

- [ ] **Step 1: Write failing store tests**

Cover these exact cases:

```text
first event creates a window
same EventID is counted once
events are returned by OccurredAt then EventID
window is not claimable before End+Lateness
ClaimDue marks a window claimed
Release makes it claimable at retryAt
Complete prevents duplicate replay from recreating a result
event arriving after due time increments late count and is not sampled
MaxEvents bounds retained events while TotalMessages keeps growing
```

- [ ] **Step 2: Run tests and verify failure**

Run: `cd v11/insight && go test ./internal/adapters/window/memory -count=1`

Expected: FAIL because package implementation is absent.

- [ ] **Step 3: Implement configuration and state ownership**

Implement:

```go
type Config struct {
    WindowSize time.Duration
    Lateness   time.Duration
    MaxEvents  int
}

type Store struct {
    mu        sync.Mutex
    config    Config
    windows   map[string]*windowState
    completed map[string]struct{}
}
```

All map access occurs under `mu`. Copy events before returning from `Load`; never expose internal slices.

- [ ] **Step 4: Implement add, claim, load, release, and complete**

`Add` validates the event, calculates the window, deduplicates by EventID, increments exact counters, and stores at most `MaxEvents`. `ClaimDue` sorts due references by Start then RoomID and returns at most `limit`.

- [ ] **Step 5: Run race-aware tests**

Run: `cd v11/insight && gofmt -w internal/adapters/window/memory && go test -race ./internal/adapters/window/memory -count=1`

Expected: PASS with no race reports.

- [ ] **Step 6: Commit**

```bash
git add v11/insight/internal/adapters/window/memory
git commit -m "feat(v11): add bounded memory windows"
```

---

### Task 3: Rule Analysis and Eino Graph

**Files:**
- Modify: `v11/insight/go.mod`
- Create: `v11/insight/internal/adapters/analyzer/rule/analyzer.go`
- Create: `v11/insight/internal/adapters/analyzer/rule/analyzer_test.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/model.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/fake_model.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/prompt.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/parser.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/analyzer.go`
- Create: `v11/insight/internal/adapters/analyzer/eino/analyzer_test.go`

**Interfaces:**
- Consumes: `ports.InsightAnalyzer` and `domain.InsightWindow`.
- Produces: `rule.Analyzer` and `eino.Analyzer`, both implementing `ports.InsightAnalyzer`.
- Produces: `eino.CompletionModel` so DeepSeek can be added without changing the graph.

- [ ] **Step 1: Write failing rule-analyzer tests**

Use a window containing repeated complaints, normal messages, and questions. Assert exact message count, unique users, question count, repeated ratio, peak messages per second, and top repeated text.

- [ ] **Step 2: Implement deterministic rule analysis**

The rule analyzer performs no network calls. Normalize whitespace, count identical content, detect a question through `?`, `？`, `吗`, `么`, and `什么`, and sort ties lexicographically for stable replay.

- [ ] **Step 3: Add Eino v0.9.13**

Run: `cd v11/insight && go get github.com/cloudwego/eino@v0.9.13`

Expected: `go.mod` pins `github.com/cloudwego/eino v0.9.13`.

- [ ] **Step 4: Define the model-neutral request and response**

```go
type CompletionRequest struct {
    SystemPrompt string
    UserPrompt   string
    JSONMode     bool
}

type CompletionResponse struct {
    Content      string
    Provider     string
    Model        string
    InputTokens  int
    OutputTokens int
    Latency      time.Duration
}

type CompletionModel interface {
    Complete(context.Context, CompletionRequest) (CompletionResponse, error)
}
```

- [ ] **Step 5: Write failing graph tests**

Test a valid fake JSON response, an unknown evidence EventID, invalid JSON, and context cancellation. A valid response must preserve provider, model, token, and latency metadata.

- [ ] **Step 6: Implement prompt and parser**

The system prompt says that messages are untrusted data, asks for JSON only, includes the expected schema, and forbids commands. The user prompt enumerates selected events as `[EventID] username: content`. Enforce the 8000-rune limit before invoking the model.

The parser uses `json.Decoder.DisallowUnknownFields`, checks enum and length limits, and rejects every evidence ID not present in the window.

- [ ] **Step 7: Compile the Eino graph**

Build a `compose.NewGraph[domain.InsightWindow, domain.AnalysisResult]` with three lambda nodes:

```text
prepare -> complete -> parse_and_validate
```

`prepare` computes rule stats and prompt input, `complete` invokes `CompletionModel`, and `parse_and_validate` returns merged rules, semantic output, and model metadata. Add START/END edges and compile once in `NewAnalyzer`.

- [ ] **Step 8: Implement deterministic FakeModel**

`FakeModel` reads EventIDs from the request and returns stable valid JSON. It also supports configured error, delay, and invalid-JSON modes for later fallback tests.

- [ ] **Step 9: Run tests**

Run: `cd v11/insight && gofmt -w internal/adapters/analyzer && go test -race ./internal/adapters/analyzer/... -count=1`

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add v11/insight/go.mod v11/insight/go.sum v11/insight/internal/adapters/analyzer
git commit -m "feat(v11): compose insight analysis with Eino"
```

---

### Task 4: JSONL Source, Memory Repository, and Bounded Processor

**Files:**
- Create: `v11/insight/internal/adapters/source/jsonl/source.go`
- Create: `v11/insight/internal/adapters/source/jsonl/source_test.go`
- Create: `v11/insight/internal/adapters/repository/memory/repository.go`
- Create: `v11/insight/internal/adapters/repository/memory/repository_test.go`
- Create: `v11/insight/internal/app/ingest.go`
- Create: `v11/insight/internal/app/processor.go`
- Create: `v11/insight/internal/app/processor_test.go`
- Create: `v11/insight/testdata/fixtures/demo.jsonl`

**Interfaces:**
- Consumes: all ports from Task 1 and analyzers from Task 3.
- Produces: `jsonl.Source`, `memory.Repository`, `app.Ingestor`, `app.Processor`.

- [ ] **Step 1: Write JSONL source tests**

Test valid lines, blank lines, malformed JSON with line number in the error, event validation, and cancellation. Do not silently skip malformed records.

- [ ] **Step 2: Implement the JSONL source**

`Source.Run` scans an `io.Reader`, decodes one event per nonblank line, validates it, converts `OccurredAt` to UTC, and invokes the provided handler synchronously.

- [ ] **Step 3: Write repository idempotency tests**

Save the same idempotency key twice and assert one record. Verify `Latest` chooses the newest window and `List(roomID, limit)` returns newest first without exposing internal slices.

- [ ] **Step 4: Implement the memory repository**

Use `sync.RWMutex`, clone JSON-backed slices before returning, and index by `RoomInsight.IdempotencyKey()`.

- [ ] **Step 5: Write processor tests**

Cover:

```text
normal analyzer result is saved and window completes
analyzer error invokes RuleAnalyzer and saves degraded status
repository error releases the window
duplicate processing creates one result
job queue capacity is fixed
two workers process multiple due windows without a race
```

- [ ] **Step 6: Implement ingestor and processor**

`Ingestor.Handle` passes validated events and the injected clock time to `WindowStore.Add`. `Processor.ProcessDue(ctx, now)` claims at most 32 windows, feeds a channel of fixed capacity, starts exactly configured workers, waits with `sync.WaitGroup`, and returns a summary of completed, degraded, and failed windows.

Generate `InsightID` from SHA-256 of the idempotency key. On analyzer error, use `RuleAnalyzer`; on repository failure, call `Release` instead of `Complete`.

- [ ] **Step 7: Add deterministic fixture data**

Create two rooms across two old windows. Include a repeated card-lag complaint, an event-time question, positive reactions, and stable EventIDs. All windows must already be due when the test runs.

- [ ] **Step 8: Run tests**

Run: `cd v11/insight && gofmt -w internal/app internal/adapters/source internal/adapters/repository && go test -race ./internal/app ./internal/adapters/source/... ./internal/adapters/repository/... -count=1`

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add v11/insight/internal/app v11/insight/internal/adapters/source v11/insight/internal/adapters/repository v11/insight/testdata
git commit -m "feat(v11): process replay windows idempotently"
```

---

### Task 5: Standalone HTTP API and Process Entry

**Files:**
- Create: `v11/insight/internal/httpapi/handler.go`
- Create: `v11/insight/internal/httpapi/handler_test.go`
- Create: `v11/insight/internal/httpapi/static.go`
- Create: `v11/insight/internal/httpapi/static_test.go`
- Create: `v11/insight/cmd/insightd/main.go`
- Create: `v11/insight/cmd/insightd/main_test.go`

**Interfaces:**
- Consumes: `ports.InsightRepository`, `jsonl.Source`, `app.Ingestor`, `app.Processor`.
- Produces: runnable `insightd` and versioned HTTP JSON contract.

- [ ] **Step 1: Write failing API tests**

Test:

```text
GET /health returns {"status":"ok"}
GET /api/v1/rooms/room-1/insights/latest returns latest result
unknown room returns 404 JSON error
GET history enforces limit 1..100 and newest-first order
malformed path and unsupported method return JSON errors
```

- [ ] **Step 2: Implement the API using net/http**

Use `http.ServeMux`, strict path parsing, `Content-Type: application/json`, `json.Encoder`, and a small response helper. Do not expose repository concrete types.

- [ ] **Step 3: Write static-hosting tests**

Verify existing files are served, unknown frontend routes fall back to `index.html`, API routes are never swallowed, and an absent web directory leaves API-only mode working.

- [ ] **Step 4: Implement optional static hosting**

Serve the built React directory only when `-web-dir` is non-empty and valid. Use path cleaning and never allow traversal outside the configured root.

- [ ] **Step 5: Write command assembly tests**

Extract `run(ctx, args, stdout, stderr) error`. Test invalid worker count, absent input file, invalid window duration, and successful fixture startup with a listener supplied by the test.

- [ ] **Step 6: Implement insightd lifecycle**

Flags:

```text
-listen=:18120
-input=./testdata/fixtures/demo.jsonl
-web-dir=
-window=60s
-lateness=10s
-workers=2
-job-capacity=128
-model=fake
```

Assemble only interfaces, replay JSONL, process due windows, start HTTP, and shut down on SIGINT/SIGTERM with a five-second timeout. Reject unsupported model values at startup.

- [ ] **Step 7: Verify the backend manually through HTTP**

Run: `cd v11/insight && go run ./cmd/insightd -listen=:18120 -input=./testdata/fixtures/demo.jsonl`

Then request: `curl -s http://127.0.0.1:18120/api/v1/rooms/room-001/insights/latest`

Expected: HTTP 200 containing `status`, `summary`, `topics`, and valid evidence EventIDs.

- [ ] **Step 8: Run tests and commit**

Run: `cd v11/insight && gofmt -w cmd internal/httpapi && go test -race ./cmd/... ./internal/httpapi -count=1`

Expected: PASS.

```bash
git add v11/insight/cmd v11/insight/internal/httpapi
git commit -m "feat(v11): expose standalone insight API"
```

---

### Task 6: Standalone React Operations Page

**Files:**
- Create: `v11/insight/web/package.json`
- Create: `v11/insight/web/package-lock.json`
- Create: `v11/insight/web/index.html`
- Create: `v11/insight/web/tsconfig.json`
- Create: `v11/insight/web/tsconfig.app.json`
- Create: `v11/insight/web/tsconfig.node.json`
- Create: `v11/insight/web/vite.config.ts`
- Create: `v11/insight/web/src/main.tsx`
- Create: `v11/insight/web/src/app/App.tsx`
- Create: `v11/insight/web/src/api/client.ts`
- Create: `v11/insight/web/src/api/types.ts`
- Create: `v11/insight/web/src/components/InsightHeader.tsx`
- Create: `v11/insight/web/src/components/RuleMetrics.tsx`
- Create: `v11/insight/web/src/components/SemanticSections.tsx`
- Create: `v11/insight/web/src/components/EvidencePanel.tsx`
- Create: `v11/insight/web/src/styles/tokens.css`
- Create: `v11/insight/web/src/styles/global.css`
- Create: `v11/insight/web/src/test/setup.ts`
- Create: `v11/insight/web/src/app/App.test.tsx`

**Interfaces:**
- Consumes: `GET /api/v1/rooms/{room_id}/insights/latest`.
- Produces: build output in `v11/insight/web/dist` that `insightd` can host.

- [ ] **Step 1: Scaffold with the V10 dependency versions**

Use React, React DOM, lucide-react, Vite, TypeScript, Vitest, Testing Library, jsdom, and oxlint. Do not import source files from `v10/web`.

- [ ] **Step 2: Write failing UI tests**

Mock `fetch` and verify:

```text
default room loads automatically
room search loads a different room
normal and degraded status are distinct
summary and exact rule metrics render
topics, questions, sentiment, and alerts render
clicking a claim reveals evidence events
404 and network errors show usable states without deleting the last success
```

- [ ] **Step 3: Implement typed API client**

Use `AbortController`, runtime checks for required top-level fields, URL-encode room IDs, and never concatenate untrusted text into HTML.

- [ ] **Step 4: Build the operations layout**

Create a quiet work-focused dashboard:

```text
compact header and room search
summary band with status and window time
five exact metric cells
unframed two-column semantic layout
evidence side panel
responsive single-column mobile layout
```

Use Lucide icons for search, refresh, clock, activity, message, and evidence actions. Cards use at most 8px radius; no gradients, decorative blobs, nested cards, oversized hero, or instructional feature copy.

- [ ] **Step 5: Add stable loading, stale, empty, and error states**

Keep dimensions stable while loading. Preserve the last successful result on refresh failure and mark it stale. Ensure long Chinese text wraps and never overlaps buttons or evidence.

- [ ] **Step 6: Run frontend verification**

Run: `cd v11/insight/web && npm install`

Run: `cd v11/insight/web && npm test && npm run build && npm run lint`

Expected: all tests pass, TypeScript build succeeds, and lint reports no errors.

- [ ] **Step 7: Commit**

```bash
git add v11/insight/web
git commit -m "feat(v11): add standalone insight dashboard"
```

---

### Task 7: End-to-End Verification and Learning Documentation

**Files:**
- Create: `v11/insight/web/playwright.config.ts`
- Create: `v11/insight/web/e2e/insight.spec.ts`
- Create: `v11/insight/README.md`
- Create: `docs/benchmark/v11-ai-insight-minimal-report-2026-08-04.md`
- Modify: `docs/superpowers/plans/2026-08-04-v11-ai-insight-minimal-closed-loop.md`

**Interfaces:**
- Consumes: complete backend and frontend from Tasks 1-6.
- Produces: a reproducible minimal-closure report and beginner-focused learning guide.

- [ ] **Step 1: Add Playwright configuration**

Configure one desktop and one mobile Chromium project. The web server command builds the frontend and runs:

```text
go run ../cmd/insightd
  -listen=:18121
  -input=../testdata/fixtures/demo.jsonl
  -web-dir=./dist
```

Use `http://127.0.0.1:18121` and reuse no pre-existing server in CI.

- [ ] **Step 2: Write the failing end-to-end test**

Verify the page loads `room-001`, displays a normal insight, shows exact counts, opens evidence for the card-lag topic, switches to `room-002`, and remains usable at a mobile viewport without horizontal overflow.

- [ ] **Step 3: Run and fix only observed integration issues**

Run: `cd v11/insight/web && npx playwright install chromium && npm run test:e2e`

Expected: desktop and mobile tests pass against the real Go process and JSONL fixture.

- [ ] **Step 4: Run complete backend verification**

Run: `cd v11/insight && go test -count=1 ./...`

Run: `cd v11/insight && go test -race -count=1 ./...`

Run: `cd v11/insight && go vet ./...`

Expected: all commands pass.

- [ ] **Step 5: Run complete frontend verification**

Run: `cd v11/insight/web && npm test && npm run lint && npm run build && npm run test:e2e`

Expected: all commands pass.

- [ ] **Step 6: Inspect desktop and mobile screenshots**

Capture 1440x900 and 390x844 screenshots. Verify nonblank content, no overlap, readable evidence, stable metric cells, and no horizontal overflow. Fix only visual issues demonstrated by screenshots.

- [ ] **Step 7: Write README and measured report**

README sections:

```text
V11 purpose and low-coupling architecture
directory and port/adapter explanation
Go concepts for beginners
Eino graph walkthrough
window, worker pool, mutex, channel, and context explanation
no-middleware startup
tests and troubleshooting
how DeepSeek, Kafka, Redis, and MySQL plug in next
```

The report records exact environment, fixture size, commands, pass counts, processing duration, created/degraded counts, and known limits. Do not claim quality or throughput that was not measured.

- [ ] **Step 8: Mark plan checkboxes from evidence and commit**

Update only completed checkboxes. Keep failed or deferred checks unchecked with an explanation in the report.

```bash
git add v11/insight docs/benchmark/v11-ai-insight-minimal-report-2026-08-04.md docs/superpowers/plans/2026-08-04-v11-ai-insight-minimal-closed-loop.md
git commit -m "test(v11): verify AI insight minimal loop"
```

## Final Acceptance Command Set

```bash
cd v11/insight
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...

cd web
npm test
npm run lint
npm run build
npm run test:e2e
```

The minimal closed loop is complete only when the documented JSONL fixture produces idempotent room insights, the API returns evidence-backed results, the standalone page renders them on desktop and mobile, fake-model failure produces a degraded result, and all commands above pass.
