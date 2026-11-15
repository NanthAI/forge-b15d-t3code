import { expect, it } from "vite-plus/test";
import { overlapTask2 } from "./overlap-task2.ts";

it("returns overlapTask2", () => {
  expect(overlapTask2()).toBe("overlapTask2");
});
