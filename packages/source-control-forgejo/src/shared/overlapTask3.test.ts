import { assert, describe, it } from "@effect/vitest";

import { overlapTask3 } from "./overlapTask3.ts";

describe("overlapTask3", () => {
  it("returns its name", () => {
    assert.strictEqual(overlapTask3(), "overlapTask3");
  });
});
