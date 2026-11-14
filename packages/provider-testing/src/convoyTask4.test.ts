import { describe, expect, it } from "vite-plus/test";

import { convoyTask4 } from "./convoyTask4.ts";

describe("convoyTask4", () => {
  it("returns its name", () => {
    expect(convoyTask4()).toBe("convoyTask4");
  });
});
