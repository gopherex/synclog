export {
  SyncClient,
  type SyncBinding,
  type SyncCatchUpSummary,
  type SyncClientOptions,
  type SyncProjection,
  type SyncSubscription,
} from "./client.js";
export { MemorySyncStore, type MemorySyncRecord } from "./memory-store.js";
export {
  type DecodedGatewayEvent,
  type DecodedGatewaySnapshot,
  type ProjectionStore,
} from "./store.js";
export {
  defaultBindingKey,
  normalizeBindingKey,
  targetBindingKey,
  targetKey,
  type TargetBindingKey,
} from "./target.js";
export type { SyncGatewayTransport } from "./transport.js";
export * from "./proto/synclog/v1/gateway_pb.js";
export * from "./proto/synclog/v1/service_pb.js";
export * from "./proto/synclog/v1/snapshot_pb.js";
export * from "./proto/synclog/v1/event_pb.js";
