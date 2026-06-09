import type { SyncTarget } from "./proto/synclog/v1/gateway_pb.js";

export const defaultBindingKey = "default";

export type TargetBindingKey = {
  subscriberId: string;
  target: SyncTarget;
  bindingKey: string;
};

export function normalizeBindingKey(bindingKey: string | undefined): string {
  return bindingKey && bindingKey.length > 0 ? bindingKey : defaultBindingKey;
}

export function targetKey(target: SyncTarget): string {
  return `${target.namespace}/${target.id}/${target.view}`;
}

export function targetBindingKey(key: TargetBindingKey): string {
  return `${key.subscriberId}:${targetKey(key.target)}#${key.bindingKey}`;
}
