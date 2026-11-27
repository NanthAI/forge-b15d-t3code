import { describe, expect, it } from "vite-plus/test";

import { overlapTask7 } from "./overlapTask7.ts";

describe("overlapTask7", () => {
  it("returns its name", () => {
    expect(overlapTask7()).toBe("overlapTask7");
  });
});
