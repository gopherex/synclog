# synclog

`synclog` is a library-first sync engine for durable cursor-based event streams.
It provides generic sync mechanics so products do not reimplement catch-up, live
subscription, ack, cursor storage, snapshots, and too-long recovery for every
entity type.

It is not a domain model, projection engine, chat protocol, or mandatory
standalone server. The primary deployment model is embedding the Go library into
a product backend and mounting whichever transport handlers the product wants.

```text
frontend
  -> product backend
       -> embedded synclog gateway
            -> synclog core event log
            -> cursor store
            -> snapshot store
            -> product auth/target hooks
```

Clients address public `SyncTarget { namespace, id, view }` values. Raw
`stream_id`, retention internals, sharding, and admin controls stay behind the
gateway.

## Packages

Go:

- `pkg/synclog` - core stream/cursor mechanics and storage contracts.
- `pkg/gateway` - target-based gateway engine and product hooks.
- `pkg/gateway/transport/grpc` - gRPC adapter for the gateway proto service.
- `pkg/synclog/transport/grpc` - raw core/snapshot/admin gRPC adapters.
- `pkg/proto/synclog/v1` - generated Go protobuf/gRPC types.
- `internal/storage/memory` - in-memory reference adapter used by tests.
- `internal/storage/contract` - reusable storage contract tests for adapters.

TypeScript:

- `@gopherex/synclog` - generated proto types plus transport-free frontend sync
  runtime.
- `SyncGatewayTransport` - frontend network boundary; implement it with grpc-web,
  Connect, WebSocket, SSE, fetch, React Native bridge, or any other transport.
- `ProjectionStore` - frontend local projection boundary.
- `MemorySyncStore` - reference/test store only.

## Install

### Go

```bash
go get github.com/gopherex/synclog
```

Use the Go module from product backends that embed the core/gateway engines.

### npm package

The TypeScript package is published to GitHub Packages under `@gopherex`.
Configure npm for that scope:

```ini
@gopherex:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Then install:

```bash
npm install @gopherex/synclog
```

The package exports the SDK root and generated proto modules:

```ts
import { SyncClient, MemorySyncStore } from "@gopherex/synclog";
import { SyncTargetSchema } from "@gopherex/synclog/proto/synclog/v1/gateway";
```

## Backend Embedding

Products provide storage and hooks. Storage decides durability and wake-up
mechanics; synclog only depends on interfaces.

```go
import (
    "github.com/gopherex/synclog/pkg/gateway"
    gatewaygrpc "github.com/gopherex/synclog/pkg/gateway/transport/grpc"
    "github.com/gopherex/synclog/pkg/synclog"
)

func mountSynclog(
    grpcServer grpc.ServiceRegistrar,
    eventLog synclog.EventLog,
    cursorStore synclog.CursorStore,
    snapshotStore synclog.SnapshotStore,
    streamWatcher synclog.StreamWatcher,
    hooks gateway.Hooks,
) error {
    core, err := synclog.NewEngine(eventLog, cursorStore)
    if err != nil {
        return err
    }

    gw, err := gateway.NewEngine(
        core,
        hooks,
        gateway.WithSnapshotStore(snapshotStore),
        gateway.WithStreamWatcher(streamWatcher),
    )
    if err != nil {
        return err
    }

    return gatewaygrpc.Register(
        grpcServer,
        gw,
        gatewaygrpc.WithActorExtractor(actorFromContext),
    )
}
```

The product-owned hooks are:

- `SubscriberResolver` - validates and canonicalizes subscriber id for the actor.
- `Resolver` - maps a `SyncTarget` to one or more internal stream bindings.
- `Authorizer` - checks operation-level access.
- `PayloadExposurePolicy` - checks event payload type exposure.
- `SnapshotExposurePolicy` - checks snapshot payload type exposure.
- `CodecRegistry` - checks whether a payload type/version can be decoded.

For multi-binding targets each binding must have a stable public `binding_key`.
Clients ack and request snapshots by `target + binding_key`; raw stream ids never
leave backend hooks.

## Frontend SDK Model

The frontend SDK intentionally has no built-in transport. The product supplies:

1. A `SyncGatewayTransport`.
2. A `ProjectionStore`.
3. A domain projection codec/apply model.

The SDK handles:

```text
open
catch-up
TOO_LONG -> latest snapshot -> apply snapshot -> ack snapshot seq
catch-up from snapshot seq
subscribe live
apply events before ack
redelivery filtering by locally applied seq
```

### Transport interface

```ts
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
  SyncGatewayTransport,
} from "@gopherex/synclog";

export class MyTransport implements SyncGatewayTransport {
  open(req: OpenRequest): Promise<OpenResponse> {
    return callUnary("Open", req);
  }

  catchUp(req: GatewayCatchUpRequest): Promise<GatewayCatchUpResponse> {
    return callUnary("GatewayCatchUp", req);
  }

  ack(req: GatewayAckRequest): Promise<GatewayAckResponse> {
    return callUnary("GatewayAck", req);
  }

  getLatestSnapshot(
    req: GatewayGetLatestSnapshotRequest,
  ): Promise<GatewayGetLatestSnapshotResponse> {
    return callUnary("GatewayGetLatestSnapshot", req);
  }

  subscribe(
    req: GatewaySubscribeRequest,
  ): AsyncIterable<GatewaySubscribeResponse> {
    return callServerStream("GatewaySubscribe", req);
  }
}
```

Transport implementations may use any protocol. The sync runtime only requires
the method semantics above.

### Basic frontend usage

```ts
import { create } from "@bufbuild/protobuf";
import {
  MemorySyncStore,
  SyncClient,
  SyncTargetSchema,
  type GatewayEvent,
  type SyncProjection,
} from "@gopherex/synclog";

const target = create(SyncTargetSchema, {
  namespace: "project_tasks",
  id: "777",
  view: "default",
});

type TaskListSnapshot = { items: Array<{ id: string; title: string }> };
type TaskListEvent =
  | { kind: "added"; id: string; title: string }
  | { kind: "removed"; id: string };

const projection: SyncProjection<TaskListSnapshot, TaskListEvent> = {
  snapshotType: "project_tasks.snapshot",

  decodeSnapshot(payload, version) {
    if (version !== 1) throw new Error("unsupported snapshot version");
    return JSON.parse(new TextDecoder().decode(payload)) as TaskListSnapshot;
  },

  decodeEvent(event: GatewayEvent) {
    if (event.payloadType !== "project_tasks.event") {
      throw new Error(`unsupported event type ${event.payloadType}`);
    }
    return JSON.parse(new TextDecoder().decode(event.payload)) as TaskListEvent;
  },
};

const client = new SyncClient({
  subscriberId: "browser-session-1",
  transport: new MyTransport(),
});

const store = new MemorySyncStore<TaskListSnapshot, TaskListEvent>();

await client.catchUp({
  target,
  projection,
  store,
});

const subscription = client.subscribe({
  target,
  projection,
  store,
});

// Later, for route change/logout/etc.
subscription.stop();
await subscription.done;
```

`MemorySyncStore` is for tests and demos. Browser apps should normally implement
`ProjectionStore` on top of IndexedDB or their existing durable app store so the
projection update and applied seq are persisted atomically.

### Frontend store contract

```ts
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
}
```

`applySnapshot` and `applyEvents` should atomically update product projection
state and the local applied seq. The server cursor is still authoritative, but
local applied seq lets the frontend ignore redelivered events after a crash or
network retry.

## Recovery Flow

1. Client calls gateway catch-up by target.
2. If status is `TOO_LONG`, client loads latest compatible snapshot by target.
3. Client decodes/applies the snapshot.
4. Client acks the snapshot seq.
5. Client catches up from snapshot seq.
6. Client subscribes live.

The TypeScript `SyncClient` implements this flow. Product code only supplies the
transport and projection/store logic.

## Development

Generate protobuf code:

```bash
make gen
```

Run Go checks:

```bash
make test
```

Run TypeScript build/typecheck/memory sync test:

```bash
make test-ts
```

Run everything:

```bash
make test && make test-ts
```

Future durable storage adapters should run:

```go
contract.Run(t, func(t *testing.T) contract.Store {
    return newAdapterForTest(t)
})
```

The contract verifies seq assignment, idempotency, contiguous reads, monotonic
ack, watcher wake-up, retention/too-long behavior, and snapshot lookup/listing.

## Release

The release flow follows the same shape as `gopherex/ws-proto`:

```bash
make release
```

The command:

1. Requires a clean working tree.
2. Reads the latest `vX.Y.Z` tag.
3. Lets you bump major/minor/patch.
4. Updates the npm workspace version and lockfile.
5. Commits `release vX.Y.Z`.
6. Creates and pushes tag `vX.Y.Z`.

Pushing the tag runs `.github/workflows/release.yml`:

- reuses the full CI workflow;
- creates a GitHub release;
- publishes `@gopherex/synclog` to GitHub Packages.

The same tag is the Go module version:

```bash
go get github.com/gopherex/synclog@vX.Y.Z
```

For npm consumers:

```bash
npm install @gopherex/synclog@X.Y.Z
```

## Current Status

Implemented:

- core append/read/catch-up/ack mechanics;
- per-stream monotonic seq and idempotency key deduplication;
- server-managed subscriber cursors;
- `TOO_LONG` recovery path;
- opaque snapshot storage API;
- target-based gateway with product hooks;
- gateway subscribe via watcher fallback, heartbeat, and backpressure;
- multi-binding targets with public `binding_key`;
- raw core/snapshot/admin gRPC adapters;
- TypeScript transport-free frontend runtime;
- TypeScript memory sync store test;
- CI and tag-based release workflows.

Pending:

- durable production storage adapter(s);
- product-specific transport implementation(s);
- product-specific frontend durable `ProjectionStore`.
