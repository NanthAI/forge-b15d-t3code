import { describe, expect, it } from "vite-plus/test";

import { convoyTask15 } from "./convoyTask15.ts";

describe("convoyTask15", () => {
  it("returns its task name", () => {
    expect(convoyTask15()).toBe("convoyTask15");
  });
});
