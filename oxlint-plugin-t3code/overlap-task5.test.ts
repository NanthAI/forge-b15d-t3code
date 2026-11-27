import { expect, it } from "@effect/vitest";

import { overlapTask5 } from "./overlap-task5.ts";

it("returns overlapTask5", () => {
  expect(overlapTask5()).toBe("overlapTask5");
});
