import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MessageComposer } from "./MessageComposer";

afterEach(cleanup);

function renderComposer(
  sendDanmaku = vi.fn(() => true),
  retryUntil = { danmaku: 0, like: 0 },
  sendLike = vi.fn(() => true),
) {
  render(
    <MessageComposer
      retryUntil={retryUntil}
      sendDanmaku={sendDanmaku}
      sendLike={sendLike}
    />,
  );

  return {
    content: screen.getByLabelText("弹幕内容"),
    like: screen.getByRole("button", { name: "点赞" }),
    send: screen.getByRole("button", { name: "发送弹幕" }),
  };
}

describe("MessageComposer", () => {
  it("trims content before sending and clears after success", async () => {
    const user = userEvent.setup();
    const sendDanmaku = vi.fn(() => true);
    const controls = renderComposer(sendDanmaku);

    await user.type(controls.content, "  hello signal  ");
    await user.click(controls.send);

    expect(sendDanmaku).toHaveBeenCalledWith("hello signal");
    expect(controls.content).toHaveValue("");
  });

  it("does not send blank content", async () => {
    const user = userEvent.setup();
    const sendDanmaku = vi.fn(() => true);
    const controls = renderComposer(sendDanmaku);

    await user.type(controls.content, "   ");
    await user.click(controls.send);

    expect(sendDanmaku).not.toHaveBeenCalled();
    expect(controls.content).toHaveValue("   ");
  });

  it("counts Unicode characters from 0/200 through 200/200", () => {
    const controls = renderComposer();
    const content = `${"信".repeat(198)}😀👍`;

    expect(screen.getByText("0/200")).toBeInTheDocument();
    fireEvent.change(controls.content, { target: { value: `${content}extra` } });

    expect(screen.getByText("200/200")).toBeInTheDocument();
    expect(Array.from((controls.content as HTMLTextAreaElement).value)).toHaveLength(200);
  });

  it("preserves content when sending fails", async () => {
    const user = userEvent.setup();
    const controls = renderComposer(vi.fn(() => false));

    await user.type(controls.content, "keep me");
    await user.click(controls.send);

    expect(controls.content).toHaveValue("keep me");
  });

  it("disables only danmaku during a danmaku retry window", () => {
    const controls = renderComposer(
      vi.fn(() => true),
      { danmaku: Date.now() + 5_000, like: 0 },
      vi.fn(() => true),
    );

    expect(controls.send).toBeDisabled();
    expect(controls.like).toBeEnabled();
  });

  it("disables only likes during a like retry window", () => {
    const controls = renderComposer(
      vi.fn(() => true),
      { danmaku: 0, like: Date.now() + 5_000 },
      vi.fn(() => true),
    );

    expect(controls.send).toBeEnabled();
    expect(controls.like).toBeDisabled();
  });
});
