import { expect, it, vi } from "vite-plus/test";

import { makeDesktopViewportReader } from "./DesktopViewport.ts";

it("retains the last CSS size after failed or invalid reads and reports later changes", async () => {
  const first = { width: 1200, height: 750 };
  const next = { width: 960, height: 600 };
  const measure = vi.fn<() => Promise<typeof first>>().mockResolvedValue(first);
  const changed = vi.fn();
  const read = makeDesktopViewportReader(measure, changed);
  expect(await read()).toEqual(first);
  expect(await read()).toEqual(first);
  expect(changed.mock.calls).toEqual([[first]]);

  measure.mockRejectedValueOnce(new Error("navigation interrupted the read"));
  expect(await read()).toEqual(first);
  for (const size of [
    { width: 0, height: 0 },
    { width: NaN, height: 600 },
    { width: 900, height: Infinity },
  ]) {
    measure.mockResolvedValueOnce(size);
    expect(await read()).toEqual(first);
  }
  measure.mockResolvedValueOnce(next);
  expect(await read()).toEqual(next);
  expect(changed.mock.calls).toEqual([[first], [next]]);
});

it.each([false, true])(
  "bounds waits without starting overlapping reads and accepts a late size (cached: %s)",
  async (cached) => {
    vi.useFakeTimers();
    try {
      const first = { width: 1200, height: 750 };
      const next = { width: 960, height: 600 };
      const measurement = Promise.withResolvers<typeof first>();
      const measure = vi.fn<() => Promise<typeof first>>().mockResolvedValue(first);
      const changed = vi.fn();
      const read = makeDesktopViewportReader(measure, changed);
      if (cached) expect(await read()).toEqual(first);
      measure.mockReturnValueOnce(measurement.promise);
      const a = read();
      const b = read();
      expect(a).toBe(b);
      await vi.advanceTimersByTimeAsync(2_000);
      expect(await a).toEqual(cached ? first : null);
      expect(await read()).toEqual(cached ? first : null);
      expect(measure).toHaveBeenCalledTimes(cached ? 2 : 1);

      measurement.resolve(next);
      await vi.advanceTimersByTimeAsync(0);
      expect(changed).toHaveBeenLastCalledWith(next);
      measure.mockResolvedValueOnce(next);
      expect(await read()).toEqual(next);
      expect(measure).toHaveBeenCalledTimes(cached ? 3 : 2);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  },
);
