# Embedding synclog

This is the intended non-Postgres wiring shape for a product backend. The memory
store is internal and used for tests/reference behavior; production embedding
should provide a durable adapter that satisfies the same storage contracts.

## Components

```text
product backend
  -> product auth/session middleware
  -> gateway gRPC adapter
      -> gateway.Engine
          -> product hooks
          -> synclog.Engine
          -> SnapshotStore
```

Raw core/snapshot/admin gRPC adapters are available for platform/internal use.
They do not implement product authorization; mount them only behind product
infrastructure policy.

## Gateway setup

```go
core, err := synclog.NewEngine(eventLog, cursorStore)
if err != nil {
    return err
}

gw, err := gateway.NewEngine(
    core,
    gateway.Hooks{
        SubscriberResolver: subscriberResolver,
        Resolver:           targetResolver,
        Authorizer:         authorizer,
        PayloadPolicy:      payloadPolicy,
        SnapshotPolicy:     snapshotPolicy,
        CodecRegistry: gateway.NewStaticCodecRegistry(
            gateway.CodecRegistryEntry{PayloadType: "project.event", PayloadVersion: 1},
            gateway.CodecRegistryEntry{PayloadType: "project.snapshot", PayloadVersion: 1},
        ),
    },
    gateway.WithSnapshotStore(snapshotStore),
    gateway.WithStreamWatcher(streamWatcher),
)
if err != nil {
    return err
}

err = gatewaygrpc.Register(
    grpcServer,
    gw,
    gatewaygrpc.WithActorExtractor(actorFromContext),
)
```

## Raw services setup

```go
err = syncloggrpc.RegisterAll(
    grpcServer,
    core,
    syncloggrpc.WithSnapshotStore(snapshotStore),
    syncloggrpc.WithSnapshotAdmin(snapshotAdmin),
    syncloggrpc.WithStreamRegistry(streamRegistry),
    syncloggrpc.WithStreamAdmin(streamAdmin),
    syncloggrpc.WithCursorAdmin(cursorAdmin),
    syncloggrpc.WithStreamWatcher(streamWatcher),
)
```

If an optional capability is not configured, the corresponding raw RPC returns
`codes.Unimplemented`.

## Storage adapter readiness

A durable storage adapter should implement:

- `synclog.EventLog`
- `synclog.CursorStore`
- `synclog.StreamWatcher`
- `synclog.SnapshotStore`
- `synclog.SnapshotAdmin`
- `synclog.StreamRegistry`
- `synclog.StreamAdmin`
- `synclog.CursorAdmin`

and pass:

```go
contract.Run(t, func(t *testing.T) contract.Store {
    return newAdapterForTest(t)
})
```

`StreamWatcher` is used by subscribe transports to avoid fixed polling while
idle. The remaining missing production piece is the Postgres adapter and
migrations.
