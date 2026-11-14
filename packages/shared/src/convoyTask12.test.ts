import { describe, expect, it } from "vite-plus/test";

import { convoyTask12 } from "./convoyTask12.ts";

describe("convoyTask12", () => {
  it("returns its name", () => {
    expect(convoyTask12()).toBe("convoyTask12");
  });
});
