import { describe, expect, it } from "@effect/vitest";

import { overlapTask5 } from "./overlap-task5.ts";

describe("overlapTask5", () => {
  it("returns its name", () => {
    expect(overlapTask5()).toBe("overlapTask5");
  });
});
