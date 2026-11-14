import { assert, it } from "@effect/vitest";

import { convoyTask9 } from "./convoyTask9.ts";

it("returns convoyTask9", () => {
  assert.strictEqual(convoyTask9(), "convoyTask9");
});
