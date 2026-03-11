/**
 * @section imports:externals
 */

import type { AddSnapshotListenerOptions, SnapshotService } from "@sha3/polymarket-snapshot";

/**
 * @section types
 */

export type SnapshotCollectorRuntime = Pick<SnapshotService, "addSnapshotListener" | "removeSnapshotListener" | "disconnect">;

export type SnapshotCollectorListenerOptions = AddSnapshotListenerOptions;
