import { assert, describe, it } from "@effect/vitest";

import { convoyTask11 } from "./convoyTask11.ts";

describe("convoyTask11", () => {
  it("returns its name", () => {
    assert.equal(convoyTask11(), "convoyTask11");
  });
});
