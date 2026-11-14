import { expect, it } from "vite-plus/test";

import { convoyTask3 } from "./convoyTask3.ts";

it("returns convoyTask3", () => {
  expect(convoyTask3()).toBe("convoyTask3");
});
