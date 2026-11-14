import { assert, describe, it } from "@effect/vitest";

import { convoyTask5 } from "./convoyTask5.ts";

describe("convoyTask5", () => {
  it("returns its name", () => {
    assert.strictEqual(convoyTask5(), "convoyTask5");
  });
});
