import { assert, describe, it } from "@effect/vitest";

import { overlapTask5 } from "./overlap-task5.ts";

describe("overlapTask5", () => {
  it("returns its name", () => {
    assert.strictEqual(overlapTask5(), "overlapTask5");
  });
});
