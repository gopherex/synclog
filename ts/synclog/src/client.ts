import type {
  GatewayBatch,
  GatewayCatchUpResult,
  GatewayEvent,
  GatewaySubscribeResponse,
  SyncTarget,
} from "./proto/synclog/v1/gateway_pb.js";
import { CatchUpStatus } from "./proto/synclog/v1/service_pb.js";
import type { ProjectionStore } from "./store.js";
import type { SyncGatewayTransport } from "./transport.js";
import {
  normalizeBindingKey,
  type TargetBindingKey,
} from "./target.js";

export type SyncProjection<TSnapshot, TEvent> = {
  snapshotType: string;
  maxSnapshotVersion?: number;
  decodeSnapshot(payload: Uint8Array, version: number): TSnapshot;
  decodeEvent(event: GatewayEvent): TEvent;
};

export type SyncBinding<TSnapshot, TEvent> = {
  target: SyncTarget;
  bindingKey?: string;
  projection: SyncProjection<TSnapshot, TEvent>;
  store: ProjectionStore<TSnapshot, TEvent>;
  limitPerTarget?: number;
  totalLimitPerTarget?: number;
};

export type SyncClientOptions = {
  subscriberId: string;
  transport: SyncGatewayTransport;
  batchLimitPerTarget?: number;
  maxInFlightPerTarget?: number;
  // Live subscription reconnect backoff (exponential, capped). A subscription
  // retries on transport/stream errors until stopped; a clean stream end exits.
  retryBaseDelayMs?: number;
  retryMaxDelayMs?: number;
};

export type SyncCatchUpSummary = {
  appliedEvents: number;
  appliedSnapshots: number;
  ackedSeq: bigint;
};

export type SyncSubscription = {
  stop(): void;
  done: Promise<void>;
};

export class SyncClient {
  readonly #subscriberId: string;
  readonly #transport: SyncGatewayTransport;
  readonly #batchLimitPerTarget: number;
  readonly #maxInFlightPerTarget: number;
  readonly #retryBaseDelayMs: number;
  readonly #retryMaxDelayMs: number;

  constructor(options: SyncClientOptions) {
    if (!options.subscriberId) {
      throw new Error("subscriberId is required");
    }
    this.#subscriberId = options.subscriberId;
    this.#transport = options.transport;
    this.#batchLimitPerTarget = options.batchLimitPerTarget ?? 100;
    this.#maxInFlightPerTarget = options.maxInFlightPerTarget ?? 1000;
    this.#retryBaseDelayMs = options.retryBaseDelayMs ?? 500;
    this.#retryMaxDelayMs = options.retryMaxDelayMs ?? 30_000;
  }

  async open(targets: SyncTarget[]): Promise<void> {
    await this.#transport.open({
      subscriberId: this.#subscriberId,
      targets,
    } as never);
  }

  async catchUp<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
  ): Promise<SyncCatchUpSummary> {
    const summary: SyncCatchUpSummary = {
      appliedEvents: 0,
      appliedSnapshots: 0,
      ackedSeq: 0n,
    };

    for (;;) {
      const resp = await this.#transport.catchUp({
        subscriberId: this.#subscriberId,
        targets: [binding.target],
        limitPerTarget: binding.limitPerTarget ?? this.#batchLimitPerTarget,
        totalLimitPerTarget: binding.totalLimitPerTarget ?? 0,
      } as never);

      const result = selectResult(resp.results, binding.bindingKey);
      if (!result) {
        return summary;
      }

      if (result.status === CatchUpStatus.TOO_LONG) {
        const seq = await this.#recoverFromSnapshot(binding);
        summary.appliedSnapshots++;
        summary.ackedSeq = maxSeq(summary.ackedSeq, seq);
        continue;
      }

      if (result.status !== CatchUpStatus.OK) {
        return summary;
      }

      const batch = result.batch;
      if (!batch || batch.events.length === 0) {
        return summary;
      }

      const applied = await this.#applyBatch(binding, batch);
      summary.appliedEvents += applied.count;
      summary.ackedSeq = maxSeq(summary.ackedSeq, applied.seq);

      if (batch.final) {
        return summary;
      }
    }
  }

  subscribe<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
  ): SyncSubscription {
    const controller = new AbortController();
    const done = this.#subscribeLoop(binding, controller.signal);
    return {
      stop: () => controller.abort(),
      done,
    };
  }

  async #subscribeLoop<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
    signal: AbortSignal,
  ): Promise<void> {
    let attempt = 0;
    while (!signal.aborted) {
      try {
        await this.#runSubscription(binding, signal);
        // Clean stream end (server closed the stream): stop, do not reconnect.
        return;
      } catch (err) {
        if (signal.aborted) {
          return;
        }
        attempt++;
        await this.#backoff(attempt, signal);
      }
    }
  }

  async #runSubscription<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
    signal: AbortSignal,
  ): Promise<void> {
    await this.catchUp(binding);
    const stream = this.#transport.subscribe(
      {
        subscriberId: this.#subscriberId,
        targets: [binding.target],
        batchLimitPerTarget: binding.limitPerTarget ?? this.#batchLimitPerTarget,
        maxInFlightPerTarget: this.#maxInFlightPerTarget,
      } as never,
      signal,
    );

    const iterator = stream[Symbol.asyncIterator]();
    // Aborting an idle for-await would not interrupt the pending next(); calling
    // iterator.return() tears the generator/stream down promptly on stop().
    const onAbort = () => {
      void iterator.return?.(undefined);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      for (;;) {
        if (signal.aborted) {
          return;
        }
        const next = await iterator.next();
        if (next.done) {
          return;
        }
        if (signal.aborted) {
          return;
        }
        await this.#handleSubscribeResponse(binding, next.value);
      }
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  #backoff(attempt: number, signal: AbortSignal): Promise<void> {
    const delay = Math.min(
      this.#retryMaxDelayMs,
      this.#retryBaseDelayMs * 2 ** (attempt - 1),
    );
    return new Promise<void>((resolve) => {
      let timer: ReturnType<typeof setTimeout>;
      const onAbort = () => {
        clearTimeout(timer);
        resolve();
      };
      timer = setTimeout(() => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      }, delay);
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  async #handleSubscribeResponse<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
    resp: GatewaySubscribeResponse,
  ): Promise<void> {
    if (resp.heartbeat) {
      return;
    }
    if (!matchesBinding(resp.bindingKey, binding.bindingKey)) {
      return;
    }
    if (resp.status === CatchUpStatus.TOO_LONG) {
      await this.#recoverFromSnapshot(binding);
      await this.catchUp(binding);
      return;
    }
    if (resp.status !== CatchUpStatus.OK || !resp.batch || resp.batch.events.length === 0) {
      return;
    }
    await this.#applyBatch(binding, resp.batch);
  }

  async #recoverFromSnapshot<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
  ): Promise<bigint> {
    const snapshotResp = await this.#transport.getLatestSnapshot({
      subscriberId: this.#subscriberId,
      target: binding.target,
      payloadType: binding.projection.snapshotType,
      maxPayloadVersion: binding.projection.maxSnapshotVersion ?? 0,
      bindingKey: binding.bindingKey ?? "",
    } as never);
    const snapshot = snapshotResp.snapshot;
    if (!snapshot) {
      throw new Error("gateway returned TOO_LONG but no compatible snapshot");
    }
    const key = this.#key(binding, snapshot.bindingKey);
    const decoded = binding.projection.decodeSnapshot(
      snapshot.payload,
      snapshot.payloadVersion,
    );
    await binding.store.applySnapshot({
      key,
      snapshot: {
        seq: snapshot.seq,
        payloadType: snapshot.payloadType,
        payloadVersion: snapshot.payloadVersion,
        raw: snapshot,
        value: decoded,
      },
    });
    await this.#ack(key, snapshot.seq);
    return snapshot.seq;
  }

  async #applyBatch<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
    batch: GatewayBatch,
  ): Promise<{ count: number; seq: bigint }> {
    const key = this.#key(binding, batch.bindingKey);
    const appliedSeq = await binding.store.getAppliedSeq(key);
    const events = batch.events
      .filter((event) => event.seq > appliedSeq)
      .map((event) => ({
        seq: event.seq,
        payloadType: event.payloadType,
        payloadVersion: event.payloadVersion,
        raw: event,
        value: binding.projection.decodeEvent(event),
      }));

    if (events.length > 0) {
      await binding.store.applyEvents({ key, events });
    }
    // Ack the seq the store durably reached, never batch.seq directly: acking
    // ahead of applied state would let the server cursor skip events that the
    // projection never persisted, causing silent gaps after a crash/retry.
    const ackedSeq = await binding.store.getAppliedSeq(key);
    await this.#ack(key, ackedSeq);
    return { count: events.length, seq: ackedSeq };
  }

  async #ack(key: TargetBindingKey, seq: bigint): Promise<void> {
    await this.#transport.ack({
      subscriberId: this.#subscriberId,
      target: key.target,
      bindingKey: key.bindingKey,
      seq,
      metadata: "",
    } as never);
  }

  #key<TSnapshot, TEvent>(
    binding: SyncBinding<TSnapshot, TEvent>,
    responseBindingKey: string | undefined,
  ): TargetBindingKey {
    return {
      subscriberId: this.#subscriberId,
      target: binding.target,
      bindingKey: normalizeBindingKey(binding.bindingKey ?? responseBindingKey),
    };
  }
}

function selectResult(
  results: GatewayCatchUpResult[],
  requestedBindingKey: string | undefined,
): GatewayCatchUpResult | undefined {
  if (requestedBindingKey) {
    return results.find((result) => result.bindingKey === requestedBindingKey);
  }
  if (results.length <= 1) {
    return results[0];
  }
  throw new Error("bindingKey is required when target returns multiple bindings");
}

function matchesBinding(
  responseBindingKey: string,
  requestedBindingKey: string | undefined,
): boolean {
  if (!requestedBindingKey) {
    return true;
  }
  return responseBindingKey === requestedBindingKey;
}

function maxSeq(a: bigint, b: bigint): bigint {
  return a > b ? a : b;
}
