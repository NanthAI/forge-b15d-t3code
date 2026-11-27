import { assert, describe, it } from "vite-plus/test";

import { overlapTask4 } from "./overlapTask4.ts";

describe("overlapTask4", () => {
  it("returns its name", () => {
    assert.equal(overlapTask4(), "overlapTask4");
  });
});
