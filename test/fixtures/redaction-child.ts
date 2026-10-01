import { createLogger } from "../../src/logging";

const secret = `{"refresh_token":"synthetic-six${"x".repeat(2_000)}"}`;
const fields = { message: "safe" } as { operation?: string; message: string };
Object.defineProperty(fields, "operation", { enumerable: true, get: () => { throw new Error(secret); } });
const logger = createLogger({ service: "app", dataDirectory: "/dev/null" });
await logger.log("error", fields as { operation: string; message: string });
console.log(JSON.stringify(logger.getStatus()));
