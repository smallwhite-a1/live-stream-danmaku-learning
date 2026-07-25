# Task 3 Report: V10 Web Protocol Foundation

## Status

Completed and committed as `952a6205a2eacfe8b10ab77caebe2dba6d04e5f7` (`feat: add v10 web protocol foundation`).

## Dependency Recovery

The initial three install commands were inadvertently started concurrently against `v10/web`. The three processes were confirmed to be this task's npm children, terminated, and confirmed exited. The partial manifest had runtime dependencies recorded while the dev-dependency update had been interrupted.

After finalizing `package.json`, one sequential command regenerated a consistent dependency tree and lockfile:

```text
$ npm install
added 169 packages, and audited 170 packages in 7s
```

The installer reported two high-severity audit findings. They were not remediated because dependency upgrades are outside this focused scaffold/protocol task.

## TDD Evidence

### RED

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 failed (1)
Tests  7 failed (7)
TypeError: parseServerPacket is not a function
TypeError: encodeDanmaku is not a function
TypeError: encodeLike is not a function
```

After adding the required empty-content assertion while the parser was still blank:

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 failed (1)
Tests  8 failed (8)
```

Post-commit self-review added a regression test for JSON `null` input:

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 failed (1)
Tests  1 failed | 8 passed (9)
TypeError: Cannot read properties of null (reading 'type')
```

### GREEN

After implementing the parser, encoders, and the object-shape guard:

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 passed (1)
Tests  9 passed (9)
```

## Final Verification

Commands were run sequentially after the final amendment:

```text
$ npm test
Test Files  1 passed (1)
Tests  9 passed (9)

$ npm run build
tsc -b && vite build
dist/index.html                  0.32 kB
dist/assets/index-wS7VMm18.js  190.41 kB
built in 52ms
```

## Files Changed

- `v10/web/.gitignore`
- `v10/web/index.html`
- `v10/web/package.json`
- `v10/web/package-lock.json`
- `v10/web/tsconfig.json`
- `v10/web/tsconfig.app.json`
- `v10/web/tsconfig.node.json`
- `v10/web/vite.config.ts`
- `v10/web/src/main.tsx`
- `v10/web/src/protocol/types.ts`
- `v10/web/src/protocol/parser.ts`
- `v10/web/src/protocol/parser.test.ts`
- `v10/web/src/test/setup.ts`

The unused Vite template readme, assets, favicon/icon files, styles, generated app component, and unused lint configuration were removed before final verification. `main.tsx` is intentionally a minimal buildable mount; no later page or UI work was implemented.

## Self-Review

- Reviewed the final committed diff and `git show --check`; no whitespace errors were found.
- Confirmed only the required frontend foundation files were committed under `v10/web`.
- Confirmed the Vite proxy and Vitest configuration match the task brief.
- Confirmed parser support for danmaku, stats, and control packets; malformed JSON, non-packet JSON, and unknown types return `null`.
- Confirmed the encoders trim content, reject empty danmaku, and clamp likes to `1..20`.
- Confirmed no Tasks 4-9 UI, routing, socket, metrics, or page implementation was added.

## Concerns

- `npm install` reports two high-severity dependency audit findings. No dependency remediation was performed in this task.
- The unrelated untracked `v1/` through `v9/` directories remain present and untouched.

---

## Review Follow-up: Protocol Input Validation

### RED

Focused malformed-packet and bad-like regression tests were added before the implementation change:

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 failed (1)
Tests  15 failed | 13 passed (28)
```

The failures showed known packet types being accepted with invalid `room_id` and payload fields. They also showed `encodeLike` serializing `NaN` as JSON `null`, clamping infinity to 20, and preserving fractions. JSON numeric-overflow literals (`1e999`) were used to exercise non-finite parsed values directly.

### GREEN

The parser now validates the outer room ID plus all fields read from danmaku, stats, and control payloads. It rejects non-object payloads, wrong field types, and non-finite or negative numeric IDs/counters. Optional control fields must have their declared types when present. `encodeLike` now defaults non-finite inputs to 1, truncates finite fractions, and clamps the result to 1 through 20.

```text
$ npm test -- src/protocol/parser.test.ts
Test Files  1 passed (1)
Tests  28 passed (28)
```

### Final Verification

```text
$ npm test
Test Files  1 passed (1)
Tests  28 passed (28)

$ npm run lint
oxlint .

$ npm run build
tsc -b && vite build
built in 54ms
```

### Changed Files

- `v10/web/src/protocol/parser.ts`
- `v10/web/src/protocol/parser.test.ts`
- `v10/web/package.json`
- `.superpowers/sdd/task-3-report.md`

`v10/web/package-lock.json` was not changed because updating the package script does not alter the dependency tree.

### Self-Review

- Confirmed every declared danmaku, stats, and control field exposed to the UI is guarded at the JSON parsing boundary.
- Confirmed known malformed danmaku, stats, and control packets return `null`; unknown and client-only packet types continue to return `null`.
- Confirmed emitted like counts are always integer JSON numbers in the inclusive range 1 through 20.
- Confirmed the lint script uses the already-installed `oxlint` package and no linter dependency was added.
- Ran `git diff --check`; no whitespace errors were reported.
- Confirmed source edits are restricted to `v10/web/src/protocol` and `v10/web/package.json`; no work in `v1` through `v9` or later V10 tasks was modified.
