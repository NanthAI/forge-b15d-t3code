import { describe, expect, it } from "vite-plus/test";

import { convoyTask5 } from "./convoyTask5.ts";

describe("convoyTask5", () => {
  it("returns its name", () => {
    expect(convoyTask5()).toBe("convoyTask5");
  });
});
