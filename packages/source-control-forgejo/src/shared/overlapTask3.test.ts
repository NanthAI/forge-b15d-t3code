import { describe, expect, it } from "vite-plus/test";

import { overlapTask3 } from "./overlapTask3.ts";

describe("overlapTask3", () => {
  it("returns its name", () => {
    expect(overlapTask3()).toBe("overlapTask3");
  });
});
