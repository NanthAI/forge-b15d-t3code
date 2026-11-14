import { describe, expect, it } from "vite-plus/test";
import { convoyTask1 } from "./convoy-task-1.ts";

describe("convoyTask1", () => {
  it("returns its name", () => {
    expect(convoyTask1()).toBe("convoyTask1");
  });
});
