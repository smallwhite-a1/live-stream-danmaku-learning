# Task 4 Report: WebSocket Connection and Reconnection

## Status

Completed. This task adds the V10 WebSocket URL helper and `useDanmakuSocket` lifecycle hook. Production and test changes are limited to `v10/web/src/realtime`.

## TDD Evidence

### URL helper RED

The URL contract tests were added before `url.ts`. This was the expected missing-module failure:

```text
$ npm test -- src/realtime/url.test.ts

> web@0.0.0 test
> vitest run src/realtime/url.test.ts

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 FAIL  src/realtime/url.test.ts [ src/realtime/url.test.ts ]
Error: Failed to resolve import "./url" from "src/realtime/url.test.ts". Does the file exist?

 Test Files  1 failed (1)
      Tests  no tests
```

### URL helper GREEN

After implementing `buildSocketURL` with `URL` and `URLSearchParams`:

```text
$ npm test -- src/realtime/url.test.ts

> web@0.0.0 test
> vitest run src/realtime/url.test.ts

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 Test Files  1 passed (1)
      Tests  2 passed (2)
```

### Hook RED

The full hook contract was then added as tests using a controllable `MockWebSocket` and fake timers, before the hook existed:

```text
$ npm test -- src/realtime

> web@0.0.0 test
> vitest run src/realtime

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 FAIL  src/realtime/useDanmakuSocket.test.tsx [ src/realtime/useDanmakuSocket.test.tsx ]
Error: Failed to resolve import "./useDanmakuSocket" from "src/realtime/useDanmakuSocket.test.tsx". Does the file exist?

 Test Files  1 failed | 1 passed (2)
      Tests  2 passed (2)
```

### Hook GREEN

After implementing the minimal lifecycle contract:

```text
$ npm test -- src/realtime

> web@0.0.0 test
> vitest run src/realtime

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 Test Files  2 passed (2)
      Tests  14 passed (14)
```

### Review Regression RED/GREEN

Self-review found that using the identity object itself as an effect dependency would reconnect when its values had not changed. The regression test was added first and failed as expected:

```text
$ npm test -- src/realtime/useDanmakuSocket.test.tsx

 Test Files  1 failed (1)
      Tests  1 failed | 12 passed (13)

 FAIL  src/realtime/useDanmakuSocket.test.tsx > useDanmakuSocket > keeps the socket when the identity values are unchanged
AssertionError: expected 1 to be +0 // Object.is equality

- Expected
+ Received

- 0
+ 1
```

The hook now depends on `userId`, `username`, and `roomId` individually. The focused GREEN output was:

```text
$ npm test -- src/realtime/useDanmakuSocket.test.tsx

> web@0.0.0 test
> vitest run src/realtime/useDanmakuSocket.test.tsx

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 Test Files  1 passed (1)
      Tests  13 passed (13)
```

## Final Verification

The required commands were run sequentially after the final code change:

```text
$ npm test

> web@0.0.0 test
> vitest run

 RUN  v4.1.10 /Users/liangyu/Documents/Intern/live-stream-danmaku/v10/web

 Test Files  3 passed (3)
      Tests  43 passed (43)

$ npm run lint

> web@0.0.0 lint
> oxlint .

$ npm run build

> web@0.0.0 build
> tsc -b && vite build

vite v8.1.5 building client environment for production...
transforming... 14 modules transformed.
rendering chunks...
computing gzip size...
dist/index.html                  0.32 kB | gzip:  0.23 kB
dist/assets/index-wS7VMm18.js  190.41 kB | gzip: 59.96 kB

built in 57ms
```

## Files Changed

- `v10/web/src/realtime/url.ts`
- `v10/web/src/realtime/url.test.ts`
- `v10/web/src/realtime/useDanmakuSocket.ts`
- `v10/web/src/realtime/useDanmakuSocket.test.tsx`
- `.superpowers/sdd/task-4-report.md`

## Self-Review

- URL creation uses `URL` and `URLSearchParams`, including `ws:`/`wss:` selection and encoded identity values.
- The hook holds the active socket, reconnect timer, retry attempt, and generation in refs.
- Every socket callback checks both its generation and active socket identity, so old callbacks cannot mutate current state.
- Identity changes, manual reconnects, and unmounts invalidate the active generation before closing the previous socket; cleanup clears pending retry timers.
- Unexpected closes transition to `reconnecting` and retry with `500 * 2^attempt`, capped at 10,000 ms. A successful open resets the retry attempt.
- Incoming packets are parsed through the Task 3 protocol parser, filtered to the active room, and update danmaku, stats, and control state independently. Danmaku history is capped at the newest 300 entries.
- Sends require `WebSocket.OPEN`, use the protocol encoders for packet types 101 and 103, return a boolean, and never append a speculative message.
- Tests cover encoded connection URLs, state transitions, retry timing/cap, cleanup, manual reconnect, identity changes, stale callbacks, 300-message retention, no optimistic append, and control retry deadlines.
- `git diff --check` reported no whitespace errors. Untracked `v1/` through `v9/` and the pre-existing `v10/web/src/assets/` directory were not modified or staged.

## Concerns

- The `offline` member remains available in the public status union but is not entered by this retry-forever contract; the brief specifies reconnect-on-unexpected-close and no terminal retry limit.
