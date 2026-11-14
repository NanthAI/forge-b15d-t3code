import { describe, expect, it } from "@effect/vitest";

import { convoyTask10 } from "./convoyTask10";

describe("convoyTask10", () => {
  it("returns its name", () => {
    expect(convoyTask10()).toBe("convoyTask10");
  });
});
