import { describe, expect, it } from "vite-plus/test";

import { convoyTask3 } from "./convoyTask3.ts";

describe("convoyTask3", () => {
  it("returns its name", () => {
    expect(convoyTask3()).toBe("convoyTask3");
  });
});
