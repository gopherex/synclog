import type {
  GatewayAckRequest,
  GatewayAckResponse,
  GatewayCatchUpRequest,
  GatewayCatchUpResponse,
  GatewayGetLatestSnapshotRequest,
  GatewayGetLatestSnapshotResponse,
  GatewaySubscribeRequest,
  GatewaySubscribeResponse,
  OpenRequest,
  OpenResponse,
} from "./proto/synclog/v1/gateway_pb.js";

export interface SyncGatewayTransport {
  open(req: OpenRequest): Promise<OpenResponse>;
  catchUp(req: GatewayCatchUpRequest): Promise<GatewayCatchUpResponse>;
  ack(req: GatewayAckRequest): Promise<GatewayAckResponse>;
  getLatestSnapshot(
    req: GatewayGetLatestSnapshotRequest,
  ): Promise<GatewayGetLatestSnapshotResponse>;
  // The optional signal is aborted when the caller stops the subscription.
  // Implementations should abort the underlying stream/request so an idle
  // subscription can be torn down promptly instead of leaking the connection.
  subscribe(
    req: GatewaySubscribeRequest,
    signal?: AbortSignal,
  ): AsyncIterable<GatewaySubscribeResponse>;
}
