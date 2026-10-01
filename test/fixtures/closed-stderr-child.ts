import { createLogger } from "../../src/logging";

await Bun.stdin.text();
const logger = createLogger({ service: "app", dataDirectory: "/dev/null" });
await logger.log("error", { operation: "capture.read", message: "failure" });
await logger.flush();
console.log("survived");
