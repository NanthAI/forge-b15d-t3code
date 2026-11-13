// @effect-diagnostics globalTimers:off - CDP callbacks run outside the Effect runtime.
import type { CDPSession } from "playwright-core";

type Viewport = { readonly width: number; readonly height: number };

export class ServerBrowserViewport {
  private readonly cdp: CDPSession;
  private readonly onSize: (size: Viewport) => void;
  private size: Viewport | null = null;
  private pending: Promise<Viewport | null> | null = null;

  constructor(cdp: CDPSession, onSize: (size: Viewport) => void) {
    this.cdp = cdp;
    this.onSize = onSize;
  }

  read(): Promise<Viewport | null> {
    if (this.pending) return this.pending;
    const result = Promise.withResolvers<Viewport | null>();
    this.pending = result.promise;
    // A dialog can stall CDP. Bound callers' wait, but keep one read in flight until it settles.
    const timer = setTimeout(() => result.resolve(this.size), 2_000);
    void this.cdp
      .send("Page.getLayoutMetrics")
      .then(({ cssLayoutViewport }) => {
        const width = cssLayoutViewport.clientWidth;
        const height = cssLayoutViewport.clientHeight;
        if (Number.isFinite(width) && Number.isFinite(height) && width > 0 && height > 0) {
          const size = {
            width: Math.max(1, Math.round(width)),
            height: Math.max(1, Math.round(height)),
          };
          if (this.size?.width !== size.width || this.size.height !== size.height) {
            this.size = size;
            this.onSize(size);
          }
        }
      })
      .catch(() => {})
      .finally(() => {
        clearTimeout(timer);
        this.pending = null;
        result.resolve(this.size);
      });
    return result.promise;
  }
}
