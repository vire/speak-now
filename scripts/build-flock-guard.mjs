import { access, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";

const output = join("native", "flock_guard.node");
await mkdir("native", { recursive: true });
const node = Bun.which("node");
const candidates = [process.env.NODE_API_INCLUDE, node && join(dirname(node), "..", "include", "node"), "/usr/include/node", "/usr/local/include/node"].filter(Boolean);
const include = (await Promise.all(candidates.map(async (candidate) => {
  try { await access(join(candidate, "node_api.h")); return candidate; } catch { return undefined; }
}))).find(Boolean);
if (!include) throw new Error("install Node-API headers or set NODE_API_INCLUDE to the directory containing node_api.h");
const args = process.platform === "darwin"
  ? ["-dynamiclib", "-undefined", "dynamic_lookup", "-I", include, "native/flock_guard.c", "-o", output]
  : ["-shared", "-fPIC", "-I", include, "native/flock_guard.c", "-o", output];
const child = Bun.spawn([process.env.CC ?? "cc", ...args], { stdout: "inherit", stderr: "inherit" });
if (await child.exited !== 0) throw new Error("flock guard build failed");
