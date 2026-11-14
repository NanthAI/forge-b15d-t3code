import { describe, expect, it } from "vite-plus/test";

import { convoyTask2 } from "./convoyTask2";

describe("convoyTask2", () => {
  it("returns its name", () => {
    expect(convoyTask2()).toBe("convoyTask2");
  });
});
