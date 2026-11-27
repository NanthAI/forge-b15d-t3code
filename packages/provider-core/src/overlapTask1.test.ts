import { describe, expect, it } from "vite-plus/test";

import { overlapTask1 } from "./overlapTask1.ts";

describe("overlapTask1", () => {
  it("returns its name", () => {
    expect(overlapTask1()).toBe("overlapTask1");
  });
});
