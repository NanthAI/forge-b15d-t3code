import { expect, it } from "@effect/vitest";

import { convoyTask4 } from "./convoyTask4.ts";

it("returns convoyTask4", () => {
  expect(convoyTask4()).toBe("convoyTask4");
});
