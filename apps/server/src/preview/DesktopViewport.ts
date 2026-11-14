// @effect-diagnostics globalTimers:off - Page reads run outside the Effect runtime.

type Size = { readonly width: number; readonly height: number };
const READ_TIMEOUT_MS = 2_000;

export const makeDesktopViewportReader = (
  measure: () => Promise<Size>,
  onChange: (size: Size) => void,
) => {
  let lastSize: Size | null = null;
  let pending: Promise<Size | null> | null = null;

  return () => {
    if (pending) return pending;
    pending = new Promise<Size | null>((resolve) => {
      // Timing out a waiter cannot cancel Playwright's read; keep it single-flight until it settles.
      const timer = setTimeout(() => resolve(lastSize), READ_TIMEOUT_MS);
      void Promise.resolve()
        .then(measure)
        .then((size) => {
          if (
            !Number.isFinite(size.width) ||
            !Number.isFinite(size.height) ||
            size.width <= 0 ||
            size.height <= 0
          )
            return;
          const next = {
            width: Math.max(1, Math.round(size.width)),
            height: Math.max(1, Math.round(size.height)),
          };
          if (next.width === lastSize?.width && next.height === lastSize.height) return;
          lastSize = next;
          onChange(next);
        })
        .catch(() => undefined)
        .finally(() => {
          clearTimeout(timer);
          pending = null;
          resolve(lastSize);
        });
    });
    return pending;
  };
};
