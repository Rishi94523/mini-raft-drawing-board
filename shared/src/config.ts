import { ReplicaConfig } from "./types.js";

export interface ReplicaDefaults {
  nodeId: string;
  port: number;
  peers: string[];
}

function parsePeerList(raw: string | undefined, fallback: string[]): string[] {
  if (!raw) {
    return fallback;
  }

  return raw
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
}

export function loadReplicaConfig(defaults: ReplicaDefaults): ReplicaConfig {
  return {
    nodeId: process.env.REPLICA_ID ?? defaults.nodeId,
    port: Number(process.env.PORT ?? defaults.port),
    peers: parsePeerList(process.env.PEERS, defaults.peers),
    heartbeatIntervalMs: Number(process.env.HEARTBEAT_MS ?? 150),
    electionTimeoutMinMs: Number(process.env.ELECTION_TIMEOUT_MIN_MS ?? 500),
    electionTimeoutMaxMs: Number(process.env.ELECTION_TIMEOUT_MAX_MS ?? 800)
  };
}
