# synclog design notes

## Current API stance

Gateway APIs are proto-first. `pkg/gateway.Engine` accepts and returns generated
`synclog.v1` messages directly. Product hooks also receive
`*synclogv1.SyncTarget`, so the product-facing API and hook-facing API share the
same target shape.

Core log mechanics remain small Go interfaces because they are storage mechanics:
append, read, cursor ack, and head lookup. Snapshot/admin surfaces use generated
proto request/response messages where the protocol already defines them.

## Subscriber ownership

`subscriber_id` is not trusted as an arbitrary frontend string.

Gateway always calls `SubscriberResolver.ResolveSubscriber(actor, requested)`
before resolving targets or touching core cursors. The returned subscriber id is
the canonical id used for authorization and cursor operations.

Recommended product policy:

- Browser/mobile clients may send a stable device/session subscriber id.
- Product backend must bind it to the authenticated actor.
- Gateway hooks must reject subscriber ids not owned by the actor.
- `Authorize` still receives the canonical subscriber id for operation-specific
  checks.

## Target binding model

Gateway resolution supports one or more stream bindings per public target:

```text
1 SyncTarget = N StreamBinding
```

Every binding has a product-defined `binding_key`. The key is public and stable
within the target; it is not the internal stream id. Single-binding targets may
omit it in product resolution and gateway normalizes the key to `default`.

Cursor state is exposed as:

```text
TargetState {
  target
  bindings[] { binding_key, cursor_seq, head_seq, retained_seq_start }
}
```

The legacy top-level `cursor_seq`, `head_seq`, and `retained_seq_start` fields
remain useful for single-binding targets and are aggregate compatibility fields
for multi-binding targets. Clients that sync multi-binding targets must inspect
`bindings`.

`GatewayCatchUp` and `GatewaySubscribe` emit per-binding results. `GatewayAck`
and `GatewayGetLatestSnapshot` require `binding_key` when a target resolves to
multiple bindings.

## Retention and TOO_LONG

Storage adapters must expose a retention boundary through stream head metadata:

```text
retained_seq_start = first replayable event seq
```

If committed cursor is below `retained_seq_start - 1`, catch-up returns
`TOO_LONG`. The recovery flow is:

1. Gateway catch-up returns `TOO_LONG`.
2. Client/product loads latest compatible snapshot by target.
3. Client/product applies snapshot.
4. Client acks snapshot seq through gateway.
5. Client catch-ups again from snapshot seq.

Storage contract tests verify this with truncation.

## Snapshot semantics

Snapshots are protocol-defined opaque blobs:

```text
Snapshot { ref, payload }
SnapshotRef { stream_id, seq, payload_type, payload_version, compression, checksum, ... }
```

Storage never builds or interprets snapshots. It only stores, retrieves, lists,
and deletes them. Gateway applies target access, snapshot type allow-list,
snapshot exposure policy, and codec availability checks.

## Subscribe design

`GatewaySubscribe` reuses catch-up semantics, emits batches per target binding,
waits for server-side ack progress before re-delivering the same binding, and
sends heartbeat frames while idle.

The gRPC adapter uses `synclog.StreamWatcher` when the embedded storage adapter
provides it. Without that optional capability it falls back to the configured
poll interval. The visible delivery semantics are the same either way.

Current behavior:

- Resume point: subscription must start after the canonical server-side cursor,
  not after a client-supplied raw seq.
- Backpressure: `max_in_flight_per_target` stops delivery when delivered but
  unacked events exceed the target binding budget.
- TOO_LONG during subscription: if retention moves past the subscriber cursor,
  the stream must emit `TOO_LONG` for that target binding and stop normal event
  delivery until snapshot recovery happens.
- Multi-target subscription: allowed only when every target independently
  passes subscriber ownership, target access, payload policy, and codec checks.
- Ordering: ordering is guaranteed per target binding/stream only. Cross-target
  and cross-binding ordering is not guaranteed.

Subscribe loop:

```text
for each target:
  resolve target bindings
  authorize subscribe
  loop:
    catch up each binding from its committed cursor
    emit per-binding batch or TOO_LONG
    wait for stream watcher, ack progress, new head, or heartbeat interval
```

Memory storage implements the watcher contract. Postgres should implement the
same capability with `LISTEN/NOTIFY` or another wake-up mechanism.

## Storage contract

Every durable adapter should pass `internal/storage/contract.Run`.

The contract currently verifies:

- per-stream monotonic seq assignment;
- idempotency key deduplication;
- contiguous reads and `final` flag;
- monotonic ack;
- stream watcher wake-up on append;
- truncation causing `ErrTooLong` for old cursors;
- max-events retention trimming old events;
- latest compatible snapshot lookup;
- exact snapshot lookup;
- snapshot listing by stream/type.

Postgres adapter work should start by making these tests pass.
