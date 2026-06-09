import assert from "node:assert/strict";
import {
  CatchUpStatus,
  MemorySyncStore,
  SyncClient,
  type GatewayAckRequest,
  type GatewayAckResponse,
  type GatewayCatchUpRequest,
  type GatewayCatchUpResponse,
  type GatewayGetLatestSnapshotRequest,
  type GatewayGetLatestSnapshotResponse,
  type GatewaySubscribeRequest,
  type GatewaySubscribeResponse,
  type GatewayEvent,
  type OpenRequest,
  type OpenResponse,
  type SyncGatewayTransport,
  type SyncTarget,
} from "../src/index.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function testCatchUpAppliesEventsAndAcks(): Promise<void> {
  const target = pb<SyncTarget>({
    namespace: "project_tasks",
    id: "777",
    view: "default",
  });
  const transport = new FakeTransport([
    pb<GatewayCatchUpResponse>({
      results: [
        {
          target,
          bindingKey: "default",
          status: CatchUpStatus.OK,
          batch: {
            target,
            bindingKey: "default",
            seqStart: 1n,
            seq: 2n,
            final: true,
            events: [
              event(target, 1n, "added"),
              event(target, 2n, "updated"),
            ],
          },
          state: state(target, 0n, 2n),
        },
      ],
    }),
  ]);
  const store = new MemorySyncStore<string, string>();
  const client = new SyncClient({
    subscriberId: "sub:1",
    transport,
  });

  const summary = await client.catchUp({
    target,
    projection: stringProjection("project_tasks.snapshot"),
    store,
  });

  const record = store.getRecord({
    subscriberId: "sub:1",
    target,
    bindingKey: "default",
  });
  assert.equal(summary.appliedEvents, 2);
  assert.equal(record.appliedSeq, 2n);
  assert.deepEqual(
    record.events.map((item) => item.value),
    ["added", "updated"],
  );
  assert.deepEqual(
    transport.acks.map((ack) => ack.seq),
    [2n],
  );
}

async function testTooLongRecoversFromSnapshot(): Promise<void> {
  const target = pb<SyncTarget>({
    namespace: "project_tasks",
    id: "777",
    view: "default",
  });
  const transport = new FakeTransport(
    [
      pb<GatewayCatchUpResponse>({
        results: [
          {
            target,
            bindingKey: "default",
            status: CatchUpStatus.TOO_LONG,
            state: state(target, 0n, 4n),
          },
        ],
      }),
      pb<GatewayCatchUpResponse>({
        results: [
          {
            target,
            bindingKey: "default",
            status: CatchUpStatus.OK,
            batch: {
              target,
              bindingKey: "default",
              seqStart: 4n,
              seq: 4n,
              final: true,
              events: [event(target, 4n, "after-snapshot")],
            },
            state: state(target, 3n, 4n),
          },
        ],
      }),
    ],
    pb<GatewayGetLatestSnapshotResponse>({
      snapshot: {
        target,
        bindingKey: "default",
        seq: 3n,
        payload: encoder.encode("snapshot-at-3"),
        payloadType: "project_tasks.snapshot",
        payloadVersion: 1,
        compression: 0,
        checksum: "sha256:test",
      },
      state: state(target, 0n, 4n),
    }),
  );
  const store = new MemorySyncStore<string, string>();
  const client = new SyncClient({
    subscriberId: "sub:1",
    transport,
  });

  const summary = await client.catchUp({
    target,
    projection: stringProjection("project_tasks.snapshot"),
    store,
  });

  const record = store.getRecord({
    subscriberId: "sub:1",
    target,
    bindingKey: "default",
  });
  assert.equal(summary.appliedSnapshots, 1);
  assert.equal(record.snapshot?.value, "snapshot-at-3");
  assert.deepEqual(
    record.events.map((item) => item.value),
    ["after-snapshot"],
  );
  assert.deepEqual(
    transport.acks.map((ack) => ack.seq),
    [3n, 4n],
  );
}

class FakeTransport implements SyncGatewayTransport {
  readonly acks: GatewayAckRequest[] = [];
  #catchUps: GatewayCatchUpResponse[];
  #snapshot: GatewayGetLatestSnapshotResponse | undefined;

  constructor(
    catchUps: GatewayCatchUpResponse[],
    snapshot?: GatewayGetLatestSnapshotResponse,
  ) {
    this.#catchUps = [...catchUps];
    this.#snapshot = snapshot;
  }

  async open(_req: OpenRequest): Promise<OpenResponse> {
    return pb<OpenResponse>({ targets: [] });
  }

  async catchUp(
    _req: GatewayCatchUpRequest,
  ): Promise<GatewayCatchUpResponse> {
    const next = this.#catchUps.shift();
    if (!next) {
      throw new Error("unexpected catchUp call");
    }
    return next;
  }

  async ack(req: GatewayAckRequest): Promise<GatewayAckResponse> {
    this.acks.push(req);
    return pb<GatewayAckResponse>({});
  }

  async getLatestSnapshot(
    _req: GatewayGetLatestSnapshotRequest,
  ): Promise<GatewayGetLatestSnapshotResponse> {
    const snapshot = this.#snapshot;
    if (!snapshot) {
      throw new Error("unexpected getLatestSnapshot call");
    }
    return snapshot;
  }

  async *subscribe(
    _req: GatewaySubscribeRequest,
  ): AsyncIterable<GatewaySubscribeResponse> {
    return;
  }
}

function stringProjection(snapshotType: string) {
  return {
    snapshotType,
    decodeSnapshot(payload: Uint8Array): string {
      return decoder.decode(payload);
    },
    decodeEvent(event: GatewayEvent): string {
      return decoder.decode(event.payload);
    },
  };
}

function event(target: SyncTarget, seq: bigint, value: string): GatewayEvent {
  return pb<GatewayEvent>({
    target,
    seq,
    payload: encoder.encode(value),
    payloadType: "project_tasks.event",
    payloadVersion: 1,
    createdAtUnixMs: 0n,
    bindingKey: "default",
  });
}

function state(target: SyncTarget, cursorSeq: bigint, headSeq: bigint) {
  return {
    target,
    cursorSeq,
    headSeq,
    retainedSeqStart: 1n,
    bindings: [
      {
        bindingKey: "default",
        cursorSeq,
        headSeq,
        retainedSeqStart: 1n,
      },
    ],
  };
}

function pb<T>(value: unknown): T {
  return value as T;
}

async function testSubscribeAppliesLiveEventsAndStops(): Promise<void> {
  const target = pb<SyncTarget>({
    namespace: "project_tasks",
    id: "777",
    view: "default",
  });
  const liveBatch = pb<GatewaySubscribeResponse>({
    status: CatchUpStatus.OK,
    bindingKey: "default",
    batch: {
      target,
      bindingKey: "default",
      seqStart: 1n,
      seq: 1n,
      final: true,
      events: [event(target, 1n, "live")],
    },
    state: state(target, 0n, 1n),
  });
  const transport = new LiveTransport([liveBatch]);
  const store = new MemorySyncStore<string, string>();
  const client = new SyncClient({ subscriberId: "sub:1", transport });

  const subscription = client.subscribe({
    target,
    projection: stringProjection("project_tasks.snapshot"),
    store,
  });

  // Wait until the live event has been applied to the store.
  await transport.applied;

  const key = { subscriberId: "sub:1", target, bindingKey: "default" };
  assert.equal(store.getRecord(key).appliedSeq, 1n);
  assert.deepEqual(
    store.getRecord(key).events.map((item) => item.value),
    ["live"],
  );
  assert.deepEqual(
    transport.acks.map((ack) => ack.seq),
    [1n],
  );

  // stop() must abort the (idle) stream and resolve done without hanging.
  subscription.stop();
  await subscription.done;
  assert.equal(transport.subscribeAborted, true);
}

class LiveTransport implements SyncGatewayTransport {
  readonly acks: GatewayAckRequest[] = [];
  subscribeAborted = false;
  readonly applied: Promise<void>;
  #resolveApplied!: () => void;
  readonly #responses: GatewaySubscribeResponse[];

  constructor(responses: GatewaySubscribeResponse[]) {
    this.#responses = responses;
    this.applied = new Promise<void>((resolve) => {
      this.#resolveApplied = resolve;
    });
  }

  async open(_req: OpenRequest): Promise<OpenResponse> {
    return pb<OpenResponse>({ targets: [] });
  }

  async catchUp(
    _req: GatewayCatchUpRequest,
  ): Promise<GatewayCatchUpResponse> {
    return pb<GatewayCatchUpResponse>({ results: [] });
  }

  async ack(req: GatewayAckRequest): Promise<GatewayAckResponse> {
    this.acks.push(req);
    return pb<GatewayAckResponse>({});
  }

  async getLatestSnapshot(
    _req: GatewayGetLatestSnapshotRequest,
  ): Promise<GatewayGetLatestSnapshotResponse> {
    throw new Error("unexpected getLatestSnapshot call");
  }

  async *subscribe(
    _req: GatewaySubscribeRequest,
    signal?: AbortSignal,
  ): AsyncIterable<GatewaySubscribeResponse> {
    try {
      for (const resp of this.#responses) {
        yield resp;
      }
      this.#resolveApplied();
      // Idle until the caller stops the subscription.
      await new Promise<void>((resolve) => {
        if (signal?.aborted) {
          resolve();
          return;
        }
        signal?.addEventListener("abort", () => resolve(), { once: true });
      });
    } finally {
      this.subscribeAborted = signal?.aborted ?? false;
    }
  }
}

await testCatchUpAppliesEventsAndAcks();
await testTooLongRecoversFromSnapshot();
await testSubscribeAppliesLiveEventsAndStops();
