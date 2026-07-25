import { expect, test, type Browser, type BrowserContext, type Page } from "@playwright/test";

interface Client {
  context: BrowserContext;
  page: Page;
}

async function openClient(
  browser: Browser,
  identity: { name: string; room: string; uid: string },
): Promise<Client> {
  const context = await browser.newContext();
  const page = await context.newPage();
  const search = new URLSearchParams(identity);

  await page.goto(`/?${search}`);
  await expect(page.getByText("已连接", { exact: true })).toBeVisible();

  return { context, page };
}

function messageItem(page: Page, content: string) {
  return page.locator(".message-item").filter({ hasText: content });
}

function summaryValue(page: Page, label: string) {
  return page.locator(".summary-metric").filter({ hasText: label }).locator("strong");
}

test("two clients exchange danmaku and likes through the real Go server", async ({ browser }) => {
  const room = `e2e-room-${Date.now()}`;
  const message = `双端可见-${Date.now()}`;
  const a = await openClient(browser, { name: "甲", room, uid: `e2e-a-${Date.now()}` });
  const b = await openClient(browser, { name: "乙", room, uid: `e2e-b-${Date.now()}` });

  try {
    await a.page.getByLabel("弹幕内容").fill(message);
    await a.page.getByRole("button", { name: "发送弹幕" }).click();

    await expect(messageItem(a.page, message)).toHaveCount(1);
    await expect(messageItem(b.page, message)).toHaveCount(1);

    await b.page.getByRole("button", { name: "点赞" }).click();
    await expect(summaryValue(a.page, "点赞")).toHaveText("1");
    await expect(summaryValue(b.page, "点赞")).toHaveText("1");

    await a.page.goto(`/monitor?uid=e2e-a&name=甲&room=${room}`);
    await expect(a.page.getByRole("heading", { name: "运行监控" })).toBeVisible();
    await expect(a.page.getByText("数据正常", { exact: true })).toBeVisible();
    await expect(a.page.getByText("当前接口不可观测", { exact: true })).toBeVisible();

    await b.page.goto(`/chain?uid=e2e-b&name=乙&room=${room}`);
    await expect(b.page.getByRole("heading", { name: "链路说明" })).toBeVisible();
    for (const node of [
      "浏览器",
      "WebSocket 校验与限流",
      "Manager 房间广播",
      "Redis / 本机降级",
      "Kafka Producer",
      "Consumer",
      "MySQL",
    ]) {
      await expect(b.page.getByText(node, { exact: true })).toBeVisible();
    }
  } finally {
    await Promise.all([a.context.close(), b.context.close()]);
  }
});

test("a burst receives real rate-limit feedback and recovers", async ({ browser }) => {
  const now = Date.now();
  const client = await openClient(browser, {
    name: "限流测试",
    room: `rate-room-${now}`,
    uid: `rate-user-${now}`,
  });

  try {
    const content = client.page.getByLabel("弹幕内容");
    const send = client.page.getByRole("button", { name: "发送弹幕" });

    for (let index = 0; index < 12; index += 1) {
      await content.fill(`burst-${now}-${index}`);
      await send.click();
    }

    await expect(
      client.page.getByText("发送过快，请稍后重试", { exact: true }),
    ).toBeVisible();
    await expect(send).toBeEnabled({ timeout: 3_000 });
    await expect(client.page.getByRole("button", { name: "点赞" })).toBeEnabled();
  } finally {
    await client.context.close();
  }
});

test("switching rooms closes the old realtime membership", async ({ browser }) => {
  const now = Date.now();
  const sourceRoom = `switch-source-${now}`;
  const targetRoom = `switch-target-${now}`;
  const message = `只留在原房间-${now}`;
  const a = await openClient(browser, { name: "甲", room: sourceRoom, uid: `switch-a-${now}` });
  const b = await openClient(browser, { name: "乙", room: sourceRoom, uid: `switch-b-${now}` });

  try {
    await b.page.getByRole("button", { name: "更换房间" }).click();
    await b.page.getByRole("textbox", { name: "房间", exact: true }).fill(targetRoom);
    await b.page.getByRole("button", { name: "连接房间" }).click();
    await expect(
      b.page.getByRole("banner").getByText(targetRoom, { exact: true }),
    ).toBeVisible();
    await expect(b.page.getByText("已连接", { exact: true })).toBeVisible();

    await a.page.getByLabel("弹幕内容").fill(message);
    await a.page.getByRole("button", { name: "发送弹幕" }).click();
    await expect(messageItem(a.page, message)).toHaveCount(1);
    await expect(messageItem(b.page, message)).toHaveCount(0);
  } finally {
    await Promise.all([a.context.close(), b.context.close()]);
  }
});
