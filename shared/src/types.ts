export type NodeState = "follower" | "candidate" | "leader";

export interface StrokeEvent {
  id: string;
  x0: number;
  y0: number;
  x1: number;
  y1: number;
  color: string;
  width: number;
  timestamp: number;
}

export interface LogEntry {
  term: number;
  index: number;
  stroke: StrokeEvent;
}

export interface RequestVoteRequest {
  term: number;
  candidateId: string;
}

export interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

export interface HeartbeatRequest {
  term: number;
  leaderId: string;
  leaderCommit?: number;
}

export interface HeartbeatResponse {
  term: number;
  success: boolean;
}

export interface AppendEntriesRequest {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  leaderCommit: number;
  entries: LogEntry[];
}

export interface AppendEntriesResponse {
  term: number;
  success: boolean;
  matchIndex?: number;
  reason?: string;
}

export interface SyncLogRequest {
  term: number;
  leaderId: string;
  fromIndex: number;
}

export interface SyncLogResponse {
  term: number;
  success: boolean;
  entries: LogEntry[];
  reason?: string;
}

export interface ReplicaConfig {
  nodeId: string;
  port: number;
  peers: string[];
  heartbeatIntervalMs: number;
  electionTimeoutMinMs: number;
  electionTimeoutMaxMs: number;
}

export interface ReplicaSnapshot {
  nodeId: string;
  state: NodeState;
  currentTerm: number;
  votedFor: string | null;
  leaderId: string | null;
  peers: string[];
  quorumSize: number;
  /** Index of the last committed log entry (-1 if nothing committed yet). Member 2. */
  commitIndex: number;
  /** Total number of entries in the local log. Member 2. */
  logLength: number;
}
