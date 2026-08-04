import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  use: {
    baseURL: 'http://127.0.0.1:18121',
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'desktop', use: { browserName: 'chromium', viewport: { width: 1440, height: 900 } } },
    { name: 'mobile', use: { browserName: 'chromium', viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, deviceScaleFactor: 1 } },
  ],
  webServer: {
    command: 'npm run build && go run ../cmd/insightd -listen=:18121 -input=../testdata/fixtures/demo.jsonl -web-dir=./dist',
    cwd: '.',
    reuseExistingServer: false,
    timeout: 60_000,
    url: 'http://127.0.0.1:18121/health',
  },
})
