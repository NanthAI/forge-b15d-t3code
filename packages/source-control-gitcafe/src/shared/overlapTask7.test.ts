import { expect, it } from "vite-plus/test";

import { overlapTask7 } from "./overlapTask7.ts";

it("returns overlapTask7", () => {
  expect(overlapTask7()).toBe("overlapTask7");
});
