import { describe, expect, it } from "@effect/vitest";

import { convoyTask2 } from "./convoyTask2.ts";

describe("convoyTask2", () => {
  it("returns its name", () => {
    expect(convoyTask2()).toBe("convoyTask2");
  });
});
