import { assert, it } from "@effect/vitest";

import { overlapTask5 } from "./overlap-task-5.ts";

it("returns overlapTask5", () => {
  assert.strictEqual(overlapTask5(), "overlapTask5");
});
