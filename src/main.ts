import { ServiceRuntime } from "./index.ts";

const IS_MIGRATION_MODE = process.argv.includes("--mode=migrate");
const SERVICE_RUNTIME = IS_MIGRATION_MODE ? ServiceRuntime.createMigrationRuntime() : ServiceRuntime.createDefault();

await SERVICE_RUNTIME.startServer();
