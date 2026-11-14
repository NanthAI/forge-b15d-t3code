import { describe, expect, it } from "vite-plus/test";

import { convoyTask7 } from "./convoyTask7.ts";

describe("convoyTask7", () => {
  it("returns its name", () => {
    expect(convoyTask7()).toBe("convoyTask7");
  });
});
