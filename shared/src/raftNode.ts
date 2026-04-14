import {
  AppendEntriesRequest,
  AppendEntriesResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  ReplicaConfig,
  ReplicaSnapshot,
  RequestVoteRequest,
  RequestVoteResponse,
  SyncLogRequest,
  SyncLogResponse
} from "./types.js";
import { Logger } from "./logger.js";

type TimeoutHandle = ReturnType<typeof setTimeout>;
type IntervalHandle = ReturnType<typeof setInterval>;

export class RaftNode {
  private state: ReplicaSnapshot["state"] = "follower";
  private currentTerm = 0;
  private votedFor: string | null = null;
  private leaderId: string | null = null;
  private electionTimer: TimeoutHandle | null = null;
  private heartbeatTimer: IntervalHandle | null = null;
  private stopped = false;

  constructor(
    private readonly config: ReplicaConfig,
    private readonly logger = new Logger(config.nodeId)
  ) {}

  start(): void {
    this.logger.info("Replica node starting", {
      port: this.config.port,
      peers: this.config.peers
    });
    this.stopped = false;
    this.resetElectionTimer();
  }

  stop(): void {
    this.stopped = true;
    this.clearElectionTimer();
    this.clearHeartbeatLoop();
    this.logger.info("Replica node stopped");
  }

  getSnapshot(): ReplicaSnapshot {
    return {
      nodeId: this.config.nodeId,
      state: this.state,
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      leaderId: this.leaderId,
      peers: this.config.peers,
      quorumSize: this.getQuorumSize()
    };
  }

  async handleRequestVote(payload: RequestVoteRequest): Promise<RequestVoteResponse> {
    if (payload.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        voteGranted: false
      };
    }

    if (payload.term > this.currentTerm) {
      this.becomeFollower(payload.term, null);
    }

    const canVote =
      this.votedFor === null || this.votedFor === payload.candidateId;

    if (canVote) {
      this.votedFor = payload.candidateId;
      this.resetElectionTimer();
      this.logger.info("Vote granted", {
        candidateId: payload.candidateId,
        term: this.currentTerm
      });
    }

    return {
      term: this.currentTerm,
      voteGranted: canVote
    };
  }

  async handleHeartbeat(payload: HeartbeatRequest): Promise<HeartbeatResponse> {
    if (payload.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false
      };
    }

    if (payload.term > this.currentTerm || this.state !== "follower") {
      this.becomeFollower(payload.term, payload.leaderId);
    } else {
      this.leaderId = payload.leaderId;
      this.resetElectionTimer();
    }

    return {
      term: this.currentTerm,
      success: true
    };
  }

  async handleAppendEntries(
    payload: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    if (payload.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false,
        reason: "Stale leader term"
      };
    }

    if (payload.term > this.currentTerm || this.state !== "follower") {
      this.becomeFollower(payload.term, payload.leaderId);
    } else {
      this.leaderId = payload.leaderId;
      this.resetElectionTimer();
    }

    if (payload.entries.length === 0) {
      return {
        term: this.currentTerm,
        success: true,
        matchIndex: payload.prevLogIndex
      };
    }

    return {
      term: this.currentTerm,
      success: false,
      reason: "Replication logic is reserved for Member 2"
    };
  }

  async handleSyncLog(_payload: SyncLogRequest): Promise<SyncLogResponse> {
    return {
      term: this.currentTerm,
      success: false,
      entries: [],
      reason: "Sync-log implementation is reserved for Member 2"
    };
  }

  private resetElectionTimer(): void {
    if (this.stopped) {
      return;
    }

    this.clearElectionTimer();
    const timeout = this.randomElectionTimeout();
    this.electionTimer = setTimeout(() => {
      void this.startElection();
    }, timeout);
  }

  private clearElectionTimer(): void {
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
      this.electionTimer = null;
    }
  }

  private startHeartbeatLoop(): void {
    this.clearHeartbeatLoop();
    this.heartbeatTimer = setInterval(() => {
      void this.sendHeartbeats();
    }, this.config.heartbeatIntervalMs);
  }

  private clearHeartbeatLoop(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private async startElection(): Promise<void> {
    if (this.stopped) {
      return;
    }

    this.state = "candidate";
    this.currentTerm += 1;
    this.votedFor = this.config.nodeId;
    this.leaderId = null;
    const electionTerm = this.currentTerm;
    let votes = 1;

    this.logger.info("Election started", {
      term: electionTerm,
      quorumSize: this.getQuorumSize()
    });

    this.resetElectionTimer();

    const responses = await Promise.allSettled(
      this.config.peers.map((peer) =>
        this.postJson<RequestVoteResponse>(`${peer}/request-vote`, {
          term: electionTerm,
          candidateId: this.config.nodeId
        })
      )
    );

    for (const result of responses) {
      if (this.state !== "candidate" || this.currentTerm !== electionTerm) {
        return;
      }

      if (result.status === "fulfilled") {
        if (result.value.term > this.currentTerm) {
          this.becomeFollower(result.value.term, null);
          return;
        }

        if (result.value.voteGranted) {
          votes += 1;
        }
      }
    }

    if (votes >= this.getQuorumSize()) {
      this.becomeLeader();
      return;
    }

    this.logger.warn("Election ended without majority", {
      term: electionTerm,
      votes
    });
  }

  private becomeLeader(): void {
    this.state = "leader";
    this.leaderId = this.config.nodeId;
    this.clearElectionTimer();
    this.logger.info("Leader elected", {
      term: this.currentTerm
    });
    void this.sendHeartbeats();
    this.startHeartbeatLoop();
  }

  private becomeFollower(term: number, leaderId: string | null): void {
    this.state = "follower";
    this.currentTerm = term;
    this.votedFor = null;
    this.leaderId = leaderId;
    this.clearHeartbeatLoop();
    this.resetElectionTimer();
    this.logger.info("Transitioned to follower", {
      term,
      leaderId
    });
  }

  private async sendHeartbeats(): Promise<void> {
    if (this.state !== "leader" || this.stopped) {
      return;
    }

    const heartbeat: HeartbeatRequest = {
      term: this.currentTerm,
      leaderId: this.config.nodeId
    };

    const responses = await Promise.allSettled(
      this.config.peers.map((peer) =>
        this.postJson<HeartbeatResponse>(`${peer}/heartbeat`, heartbeat)
      )
    );

    for (const result of responses) {
      if (result.status === "fulfilled" && result.value.term > this.currentTerm) {
        this.becomeFollower(result.value.term, null);
        return;
      }
    }
  }

  private getQuorumSize(): number {
    const clusterSize = this.config.peers.length + 1;
    return Math.floor(clusterSize / 2) + 1;
  }

  private randomElectionTimeout(): number {
    const min = this.config.electionTimeoutMinMs;
    const max = this.config.electionTimeoutMaxMs;
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  private async postJson<T>(url: string, payload: unknown): Promise<T> {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      throw new Error(`Request failed for ${url}: ${response.status}`);
    }

    return (await response.json()) as T;
  }
}
