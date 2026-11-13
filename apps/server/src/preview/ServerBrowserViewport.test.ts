import type { CDPSession } from "playwright-core";
import { expect, it, vi } from "vite-plus/test";

import { ServerBrowserViewport } from "./ServerBrowserViewport.ts";

const metrics = (width: number, height: number) => ({
  cssLayoutViewport: { clientWidth: width, clientHeight: height },
  layoutViewport: { clientWidth: width * 2, clientHeight: height * 2 },
});

it.each([false, true])(
  "keeps the last CSS size through failed and stalled reads (cached: %s)",
  async (cached) => {
    vi.useFakeTimers();
    try {
      const send = vi.fn().mockResolvedValue(metrics(960, 640));
      const onSize = vi.fn();
      const viewport = new ServerBrowserViewport({ send } as unknown as CDPSession, onSize);
      const expected = cached ? { width: 960, height: 640 } : null;
      if (cached) expect(await viewport.read()).toEqual(expected);

      send.mockRejectedValueOnce(new Error("Target unavailable"));
      expect(await viewport.read()).toEqual(expected);
      send.mockResolvedValueOnce(metrics(0, Number.NaN));
      expect(await viewport.read()).toEqual(expected);

      const stalled = Promise.withResolvers<ReturnType<typeof metrics>>();
      send.mockImplementationOnce(() => stalled.promise);
      const first = viewport.read();
      const count = send.mock.calls.length;
      expect(viewport.read()).toBe(first);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(await first).toEqual(expected);
      expect(await viewport.read()).toEqual(expected);
      expect(send).toHaveBeenCalledTimes(count);
      expect(onSize).toHaveBeenCalledTimes(cached ? 1 : 0);

      // A dialog's eventual reply releases the single read and preserves its good dimensions.
      send.mockRejectedValueOnce(new Error("Navigating"));
      stalled.resolve(metrics(1100, 700));
      await stalled.promise;
      await vi.advanceTimersByTimeAsync(0);
      expect(onSize).toHaveBeenLastCalledWith({ width: 1100, height: 700 });
      expect(await viewport.read()).toEqual({ width: 1100, height: 700 });
      send.mockResolvedValueOnce(metrics(1200, 800));
      expect(await viewport.read()).toEqual({ width: 1200, height: 800 });
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  },
);
