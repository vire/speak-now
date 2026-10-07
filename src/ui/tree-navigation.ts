export type TreeNode = { id: string; children: TreeNode[] };

const findNode = (nodes: TreeNode[], id: string): TreeNode | undefined => {
  for (const node of nodes) {
    if (node.id === id) return node;
    const child = findNode(node.children, id);
    if (child) return child;
  }
};

const parentOf = (nodes: TreeNode[], id: string, parent?: string): string | undefined => {
  for (const node of nodes) {
    if (node.id === id) return parent;
    const found = parentOf(node.children, id, node.id);
    if (found) return found;
  }
};

export const visibleTreeIds = (nodes: TreeNode[], expanded: Set<string>): string[] => nodes.flatMap((node) => [node.id, ...(expanded.has(node.id) ? visibleTreeIds(node.children, expanded) : [])]);

export const moveTreeFocus = (nodes: TreeNode[], expanded: Set<string>, focusId: string, key: string) => {
  const visible = visibleTreeIds(nodes, expanded);
  const position = Math.max(0, visible.indexOf(focusId));
  const node = findNode(nodes, focusId);
  const next = new Set(expanded);
  if (key === "ArrowDown") return { focusId: visible[Math.min(position + 1, visible.length - 1)] ?? focusId, expanded: next };
  if (key === "ArrowUp") return { focusId: visible[Math.max(position - 1, 0)] ?? focusId, expanded: next };
  if (key === "ArrowRight" && node?.children.length) {
    next.add(focusId);
    return { focusId: node.children[0].id, expanded: next };
  }
  if (key === "ArrowLeft") {
    if (next.has(focusId)) {
      next.delete(focusId);
      return { focusId, expanded: next };
    }
    return { focusId: parentOf(nodes, focusId) ?? focusId, expanded: next };
  }
  return { focusId, expanded: next };
};

export const reconcileTreeFocus = (nodes: TreeNode[], focusId: string, expanded: Set<string>): string => {
  if (visibleTreeIds(nodes, expanded).includes(focusId)) return focusId;
  return visibleTreeIds(nodes, expanded)[0] ?? "";
};
