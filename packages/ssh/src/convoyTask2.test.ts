import { assert, describe, it } from "@effect/vitest";

import { convoyTask2 } from "./convoyTask2.ts";

describe("convoyTask2", () => {
  it("returns its name", () => {
    assert.equal(convoyTask2(), "convoyTask2");
  });
});
