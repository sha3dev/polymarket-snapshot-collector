import { ServiceRuntime } from "./index.ts";

const SERVICE_RUNTIME = ServiceRuntime.createDefault();

await SERVICE_RUNTIME.startServer();
