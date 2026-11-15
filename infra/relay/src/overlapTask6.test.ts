import { describe, expect, it } from "vite-plus/test";

import { overlapTask6 } from "./overlapTask6.ts";

describe("overlapTask6", () => {
  it("returns its task name", () => {
    expect(overlapTask6()).toBe("overlapTask6");
  });
});
