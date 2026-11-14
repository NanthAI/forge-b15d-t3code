import { expect, it } from "vite-plus/test";
import { convoyTask1 } from "./convoyTask1";

it("returns convoyTask1", () => {
  expect(convoyTask1()).toBe("convoyTask1");
});
