import { expect, test } from '@playwright/test'
import { mkdir, writeFile } from 'node:fs/promises'
import path from 'node:path'

const artifactDir = path.resolve(process.cwd(), '..', '..', '..', '.superpowers', 'sdd', 'artifacts')

async function expectMetrics(page: import('@playwright/test').Page, values: Record<string, string>) {
  const metrics = page.getByLabel('Rule metrics')
  for (const [label, value] of Object.entries(values)) {
    await expect(metrics.locator('.metric').filter({ hasText: label }).locator('dd')).toHaveText(value)
  }
}

test('renders fixture-backed insight evidence and switches rooms without overflow', async ({ page }, testInfo) => {
  await page.goto('/')

  await expect(page.getByText('Normal', { exact: true })).toBeVisible()
  await expect(page.getByLabel('Insight summary')).toContainText('12:01:00 - 12:02:00 UTC')
  await expectMetrics(page, {
    Messages: '2',
    'Unique users': '2',
    Questions: '1',
    'Repeated ratio': '0.0%',
    'Peak msg/s': '1',
  })

  await page.getByRole('button', { name: 'View evidence for Neutral sentiment' }).click()
  await expect(page.getByRole('complementary', { name: 'Evidence' })).toContainText('alpha-1201-001')
  await page.getByRole('complementary', { name: 'Evidence' }).scrollIntoViewIfNeeded()
  const evidenceScreenshot = await page.screenshot()

  await page.getByLabel('Room ID').fill('room-beta')
  await page.getByRole('button', { name: 'Load room' }).click()
  await expect(page.getByLabel('Insight summary')).toContainText('12:01:00 - 12:02:00 UTC')
  await expectMetrics(page, {
    Messages: '2',
    'Unique users': '2',
    Questions: '1',
    'Repeated ratio': '0.0%',
    'Peak msg/s': '1',
  })
  await expect(page.getByRole('complementary', { name: 'Evidence' })).toContainText('No evidence selected.')

  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)
  expect(overflow).toBe(false)

  await mkdir(artifactDir, { recursive: true })
  await writeFile(path.join(artifactDir, `v11-${testInfo.project.name}.png`), evidenceScreenshot)
})
