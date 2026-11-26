import { chromium, type CDPSession } from "playwright-core";
import { expect, it, vi } from "vite-plus/test";

import { DesktopViewport } from "./DesktopViewport.ts";

const metrics = (width: number, height: number) => ({
  cssVisualViewport: { clientWidth: width, clientHeight: height },
});

it("measures CSS pixels for a page without a Playwright viewport", async () => {
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({ viewport: null });
    const page = await context.newPage();
    const session = await context.newCDPSession(page);
    await session.send("Emulation.setDeviceMetricsOverride", {
      width: 900,
      height: 700,
      deviceScaleFactor: 2,
      mobile: false,
    });
    const viewport = new DesktopViewport(session, () => {});
    await viewport.refresh();
    expect(page.viewportSize()).toBeNull();
    expect(viewport.size).toEqual({ width: 900, height: 700 });
  } finally {
    await browser.close();
  }
});

it("retains the last valid dimensions on failed and invalid reads and only reports changes", async () => {
  const send = vi.fn().mockResolvedValue(metrics(1100, 720));
  const changed = vi.fn();
  const viewport = new DesktopViewport({ send } as unknown as CDPSession, changed);
  await viewport.refresh();
  await viewport.refresh();
  send.mockRejectedValueOnce(new Error("navigating"));
  await viewport.refresh();
  for (const [width, height] of [
    [0, 720],
    [1100, -1],
    [NaN, 720],
    [1100, Infinity],
  ]) {
    send.mockResolvedValueOnce(metrics(width!, height!));
    await viewport.refresh();
  }
  expect(viewport.size).toEqual({ width: 1100, height: 720 });
  expect(changed).toHaveBeenCalledTimes(1);
  send.mockResolvedValueOnce(metrics(800, 600));
  await viewport.refresh();
  expect(viewport.size).toEqual({ width: 800, height: 600 });
  expect(changed).toHaveBeenCalledTimes(2);
});

it.each([false, true])(
  "bounds a blocked read without overlapping it, then recovers (cached: %s)",
  async (cached) => {
    vi.useFakeTimers();
    try {
      const send = vi.fn().mockResolvedValue(metrics(1100, 720));
      const changed = vi.fn();
      const viewport = new DesktopViewport({ send } as unknown as CDPSession, changed);
      if (cached) await viewport.refresh();
      send.mockClear();
      changed.mockClear();
      const blocked = Promise.withResolvers<ReturnType<typeof metrics>>();
      send.mockReturnValueOnce(blocked.promise);
      const first = viewport.refresh();
      expect(viewport.refresh()).toBe(first);
      await vi.advanceTimersByTimeAsync(2_000);
      await first;
      await viewport.refresh();
      expect(send).toHaveBeenCalledTimes(1);
      expect(viewport.size).toEqual(cached ? { width: 1100, height: 720 } : null);
      blocked.resolve(metrics(1000, 650));
      await blocked.promise;
      await viewport.refresh();
      expect(viewport.size).toEqual({ width: 1000, height: 650 });
      expect(changed).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  },
);
