import { expect, it } from "vite-plus/test";

import { overlapTask3 } from "./overlapTask3.ts";

it("returns overlapTask3", () => {
  expect(overlapTask3()).toBe("overlapTask3");
});
