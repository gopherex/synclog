import type {
  GatewayEvent,
  GatewaySnapshot,
  SyncTarget,
} from "./proto/synclog/v1/gateway_pb.js";
import type { TargetBindingKey } from "./target.js";

export type DecodedGatewayEvent<TEvent> = {
  seq: bigint;
  payloadType: string;
  payloadVersion: number;
  raw: GatewayEvent;
  value: TEvent;
};

export type DecodedGatewaySnapshot<TSnapshot> = {
  seq: bigint;
  payloadType: string;
  payloadVersion: number;
  raw: GatewaySnapshot;
  value: TSnapshot;
};

export interface ProjectionStore<TSnapshot, TEvent> {
  getAppliedSeq(key: TargetBindingKey): Promise<bigint>;

  applySnapshot(input: {
    key: TargetBindingKey;
    snapshot: DecodedGatewaySnapshot<TSnapshot>;
  }): Promise<void>;

  applyEvents(input: {
    key: TargetBindingKey;
    events: DecodedGatewayEvent<TEvent>[];
  }): Promise<void>;

  clearTarget?(target: SyncTarget): Promise<void>;
}
