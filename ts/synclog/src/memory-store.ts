import type {
  DecodedGatewayEvent,
  DecodedGatewaySnapshot,
  ProjectionStore,
} from "./store.js";
import {
  targetBindingKey,
  targetKey,
  type TargetBindingKey,
} from "./target.js";
import type { SyncTarget } from "./proto/synclog/v1/gateway_pb.js";

export type MemorySyncRecord<TSnapshot, TEvent> = {
  appliedSeq: bigint;
  snapshot?: DecodedGatewaySnapshot<TSnapshot>;
  events: DecodedGatewayEvent<TEvent>[];
};

export class MemorySyncStore<TSnapshot, TEvent>
  implements ProjectionStore<TSnapshot, TEvent>
{
  readonly #records = new Map<string, MemorySyncRecord<TSnapshot, TEvent>>();

  async getAppliedSeq(key: TargetBindingKey): Promise<bigint> {
    return this.#record(key).appliedSeq;
  }

  async applySnapshot(input: {
    key: TargetBindingKey;
    snapshot: DecodedGatewaySnapshot<TSnapshot>;
  }): Promise<void> {
    const encoded = targetBindingKey(input.key);
    const existing = this.#records.get(encoded);
    // Never regress applied state: if newer events already advanced past the
    // snapshot, keep them and only refresh the snapshot reference.
    if (existing && existing.appliedSeq > input.snapshot.seq) {
      existing.snapshot = input.snapshot;
      return;
    }
    this.#records.set(encoded, {
      appliedSeq: input.snapshot.seq,
      snapshot: input.snapshot,
      events: [],
    });
  }

  async applyEvents(input: {
    key: TargetBindingKey;
    events: DecodedGatewayEvent<TEvent>[];
  }): Promise<void> {
    if (input.events.length === 0) {
      return;
    }
    const record = this.#record(input.key);
    for (const event of input.events) {
      if (event.seq <= record.appliedSeq) {
        continue;
      }
      record.events.push(event);
      record.appliedSeq = event.seq;
    }
  }

  async clearTarget(target: SyncTarget): Promise<void> {
    // Keys are `${subscriberId}:${namespace}/${id}/${view}#${bindingKey}`.
    // Anchor on the `:<target>#` segment so we clear every binding of this
    // target across subscribers without matching unrelated substrings.
    const marker = `:${targetKey(target)}#`;
    for (const key of this.#records.keys()) {
      if (key.includes(marker)) {
        this.#records.delete(key);
      }
    }
  }

  getRecord(key: TargetBindingKey): MemorySyncRecord<TSnapshot, TEvent> {
    const record = this.#records.get(targetBindingKey(key));
    if (!record) {
      return { appliedSeq: 0n, events: [] };
    }
    const out: MemorySyncRecord<TSnapshot, TEvent> = {
      appliedSeq: record.appliedSeq,
      events: [...record.events],
    };
    if (record.snapshot) {
      out.snapshot = record.snapshot;
    }
    return out;
  }

  #record(key: TargetBindingKey): MemorySyncRecord<TSnapshot, TEvent> {
    const encoded = targetBindingKey(key);
    let record = this.#records.get(encoded);
    if (!record) {
      record = { appliedSeq: 0n, events: [] };
      this.#records.set(encoded, record);
    }
    return record;
  }
}
