import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  expect: {
    timeout: 10_000,
  },
  use: {
    baseURL: "http://127.0.0.1:15173",
    channel: "chrome",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: [
    {
      command: "go run ../cmd/server -port=18080 -redis=false -kafka=false",
      cwd: ".",
      reuseExistingServer: false,
      timeout: 60_000,
      url: "http://127.0.0.1:18080/health",
    },
    {
      command: "VITE_BACKEND_TARGET=http://127.0.0.1:18080 npm run dev -- --host 127.0.0.1 --port 15173",
      cwd: ".",
      reuseExistingServer: false,
      timeout: 60_000,
      url: "http://127.0.0.1:15173",
    },
  ],
});
