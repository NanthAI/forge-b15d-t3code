import { assert, describe, it } from "@effect/vitest";

import { convoyTask6 } from "./convoyTask6.ts";

describe("convoyTask6", () => {
  it("returns convoyTask6", () => {
    assert.equal(convoyTask6(), "convoyTask6");
  });
});
