import { describe, expect, it } from "@effect/vitest";

import { convoyTask8 } from "./convoyTask8.ts";

describe("convoyTask8", () => {
  it("returns its name", () => {
    expect(convoyTask8()).toBe("convoyTask8");
  });
});
