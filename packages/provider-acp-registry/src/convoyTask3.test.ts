import { expect, it } from "@effect/vitest";

import { convoyTask3 } from "./convoyTask3.ts";

it("returns convoyTask3", () => {
  expect(convoyTask3()).toBe("convoyTask3");
});
