# V11 Task 6 Report: Standalone Insight Dashboard

## Commit

- SHA: `94e8271dcc2388cde934e02e2cb31a676ed61db4`
- Message: `feat(v11): add standalone insight dashboard`

## Red / Green Evidence

- Red: `npm test` initially failed because `src/app/App.tsx` did not exist and the test suite could not resolve `./App`.
- Green: the completed suite passes all 8 App behaviors: default room loading, encoded room search, normal/degraded status, summary and five rule metrics, semantic sections, evidence EventID panel, 404 stale preservation, and network-error stale preservation.

## Verification

- `cd v11/insight/web && npm install`: completed; 125 packages installed, no vulnerabilities reported.
- `cd v11/insight/web && npm test`: passed, 1 test file and 8 tests.
- `cd v11/insight/web && npm run build`: passed; TypeScript and Vite production build completed.
- `cd v11/insight/web && npm run lint`: passed; `oxlint src vite.config.ts` exited cleanly.
- Staged diff check: `git diff --cached --check` completed without whitespace errors.

## Files

- Added Vite/React/TypeScript application setup and dependency lockfile under `v11/insight/web`.
- Added typed API contract and `fetchLatestInsight` client with URL encoding, response validation, and caller-owned `AbortController` cancellation.
- Added the compact operations UI: header, status/window summary, five rule metrics, semantic sections, EventID evidence panel, responsive styles, and load/error/empty/stale states.
- Added App-level behavioral tests and a local web `.gitignore` so `node_modules` and `dist` remain uncommitted.

## Concerns

- The page requires the existing `GET /api/v1/rooms/{room_id}/insights/latest` API to be available at the same origin; Vite alone will show its usable network-error state until the insight service is running or a development proxy is supplied.
- The minimum API contract contains EventIDs, not event message content, so the evidence panel intentionally lists only EventID labels.

## V11 Task 6 Review Follow-up

- Strengthened the runtime API validator with explicit primitive, nested object, and array-element checks for fields read by App/components. Incomplete 200 responses such as `semantic: {}` or malformed rule fields now surface the existing usable error state instead of reaching rendering and crashing.
- Made `RuleStats.top_repeated_count` required to match the Go API and updated the frontend fixture.
- Added focused App regressions for malformed semantic and rule payloads. The existing loader already aborts the active request and ignores superseded success/error/finally updates via `AbortSignal` guards, so data fetching was left unchanged and no redundant race test was added.

## Review Verification

- `cd v11/insight/web && npm test`: passed, 1 test file and 10 tests.
- `cd v11/insight/web && npm run build`: passed.
- `cd v11/insight/web && npm run lint`: passed.

## Follow-up Concerns

- The validator intentionally checks JSON shapes and enum values, but does not enforce semantic ranges such as confidence between 0 and 1; the frontend only needs the documented primitive/array/object contract to remain render-safe.
