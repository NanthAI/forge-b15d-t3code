import { expect, it } from "vite-plus/test";
import { convoyTask1 } from "./convoy-task1.ts";

it("returns convoyTask1", () => {
  expect(convoyTask1()).toBe("convoyTask1");
});
