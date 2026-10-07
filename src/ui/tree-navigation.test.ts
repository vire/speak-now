import { expect, test } from "bun:test";
import { moveTreeFocus, reconcileTreeFocus, visibleTreeIds } from "./tree-navigation";

const nodes = [
  { id: "workspace", children: [{ id: "tab", children: [{ id: "pane", children: [{ id: "participant", children: [] }] }] }] },
  { id: "workspace-2", children: [] },
];

test("keyboard tree navigation follows visible nodes and recovers focus by ID then parent", () => {
  expect(visibleTreeIds(nodes, new Set(["workspace", "tab", "pane"]))).toEqual(["workspace", "tab", "pane", "participant", "workspace-2"]);
  expect(moveTreeFocus(nodes, new Set(["workspace", "tab"]), "workspace", "ArrowRight")).toEqual({ focusId: "tab", expanded: new Set(["workspace", "tab"]) });
  expect(moveTreeFocus(nodes, new Set(["workspace", "tab"]), "tab", "ArrowLeft")).toEqual({ focusId: "tab", expanded: new Set(["workspace"]) });
  expect(moveTreeFocus(nodes, new Set(["workspace"]), "workspace", "ArrowDown").focusId).toBe("tab");
  expect(reconcileTreeFocus(nodes, "participant", new Set(["workspace", "tab", "pane"]))).toBe("participant");
  expect(reconcileTreeFocus(nodes, "removed", new Set(["workspace", "tab"]), { removed: "tab", parent: "workspace" })).toBe("workspace");
});
