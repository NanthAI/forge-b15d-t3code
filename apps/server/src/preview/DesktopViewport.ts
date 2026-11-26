// @effect-diagnostics globalTimers:off -- CDP callbacks run outside the Effect runtime.
import type { CDPSession } from "playwright-core";

const READ_TIMEOUT_MS = 2_000;

export class DesktopViewport {
  size: { width: number; height: number } | null = null;
  private pending: Promise<void> | null = null;

  private readonly session: CDPSession;
  private readonly onChange: () => void;

  constructor(session: CDPSession, onChange: () => void) {
    this.session = session;
    this.onChange = onChange;
  }

  refresh(): Promise<void> {
    if (this.pending) return this.pending;
    let timer: ReturnType<typeof setTimeout>;
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, READ_TIMEOUT_MS);
    });
    const read = this.session
      .send("Page.getLayoutMetrics")
      .then(({ cssVisualViewport: { clientWidth: width, clientHeight: height } }) => {
        if (!Number.isFinite(width) || !Number.isFinite(height) || width <= 0 || height <= 0)
          return;
        if (this.size?.width === width && this.size.height === height) return;
        this.size = { width, height };
        this.onChange();
      })
      .catch(() => {})
      .finally(() => {
        clearTimeout(timer);
        this.pending = null;
      });
    // A dialog can stall CDP beyond our timeout; keep sharing the read until it actually settles.
    this.pending = Promise.race([read, timeout]);
    return this.pending;
  }
}
