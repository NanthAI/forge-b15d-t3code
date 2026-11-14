import { assert, it } from "@effect/vitest";

import { convoyTask11 } from "./convoyTask11.ts";

it("returns convoyTask11", () => {
  assert.equal(convoyTask11(), "convoyTask11");
});
