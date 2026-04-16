import {
  AppendEntriesRequest,
  AppendEntriesResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  LogEntry,
  ReplicaConfig,
  ReplicaSnapshot,
  RequestVoteRequest,
  RequestVoteResponse,
  StrokeEvent,
  SyncLogRequest,
  SyncLogResponse
} from "./types.js";
import { Logger } from "./logger.js";
import { StrokeLog } from "./strokeLog.js";

type TimeoutHandle = ReturnType<typeof setTimeout>;
type IntervalHandle = ReturnType<typeof setInterval>;

export class RaftNode {
  // ─── Member 1: election state ─────────────────────────────────────────────
  private state: ReplicaSnapshot["state"] = "follower";
  private currentTerm = 0;
  private votedFor: string | null = null;
  private leaderId: string | null = null;
  private electionTimer: TimeoutHandle | null = null;
  private heartbeatTimer: IntervalHandle | null = null;
  private stopped = false;

  // ─── Member 2: replication state ──────────────────────────────────────────
  private log = new StrokeLog();
  /**
   * Index of the highest log entry known to be committed.
   * -1 = nothing committed yet (log is 0-indexed).
   */
  private commitIndex = -1;
  /**
   * For each peer, the next log index the leader will send.
   * Initialized to log.lastIndex + 1 when this node becomes leader.
   */
  private nextIndex: Map<string, number> = new Map();
  /**
   * For each peer, the highest log index known to be replicated on that peer.
   * Used to advance commitIndex once a majority has acknowledged an entry.
   */
  private matchIndex: Map<string, number> = new Map();

  constructor(
    private readonly config: ReplicaConfig,
    private readonly logger = new Logger(config.nodeId)
  ) {}

  // ─── lifecycle ────────────────────────────────────────────────────────────

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

  // ─── snapshot ─────────────────────────────────────────────────────────────

  getSnapshot(): ReplicaSnapshot {
    return {
      nodeId: this.config.nodeId,
      state: this.state,
      currentTerm: this.currentTerm,
      votedFor: this.votedFor,
      leaderId: this.leaderId,
      peers: this.config.peers,
      quorumSize: this.getQuorumSize(),
      // Member 2 fields
      commitIndex: this.commitIndex,
      logLength: this.log.length
    };
  }

  // ─── Member 1: RPC handlers ───────────────────────────────────────────────

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

  // ─── Member 2: append-entries ─────────────────────────────────────────────

  async handleAppendEntries(
    payload: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    // 1. Reject stale leader.
    if (payload.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false,
        reason: "Stale leader term"
      };
    }

    // 2. Update term / step down if needed (Member 1 pattern).
    if (payload.term > this.currentTerm || this.state !== "follower") {
      this.becomeFollower(payload.term, payload.leaderId);
    } else {
      this.leaderId = payload.leaderId;
      this.resetElectionTimer();
    }

    // 3. Empty entries = leader heartbeat with commit update only.
    if (payload.entries.length === 0) {
      this.maybeAdvanceCommitIndexAsFollower(payload.leaderCommit);
      return {
        term: this.currentTerm,
        success: true,
        matchIndex: this.log.lastIndex
      };
    }

    // 4. Log consistency check (§5.3).
    //    The entry immediately before the new batch must already be present
    //    with the correct term.
    if (payload.prevLogIndex >= 0) {
      const prevEntry = this.log.getAt(payload.prevLogIndex);
      if (prevEntry === undefined) {
        this.logger.warn("AppendEntries rejected: missing prevLogIndex", {
          prevLogIndex: payload.prevLogIndex,
          localLastIndex: this.log.lastIndex
        });
        return {
          term: this.currentTerm,
          success: false,
          reason: `Missing prevLogIndex ${payload.prevLogIndex}`
        };
      }
      // Note: prevLogTerm is not in the current AppendEntriesRequest type
      // (the frozen contract omits it to keep things simple for this project).
      // We rely on index-presence as the consistency gate, which is sufficient
      // for a 3-node demo cluster.
    }

    // 5. Append / overwrite new entries.
    for (const incoming of payload.entries) {
      if (this.log.hasMatchingEntry(incoming)) {
        // Already present and identical — idempotent, skip.
        continue;
      }

      // Conflict at this index: truncate log up to (but not including) this
      // index, then append the new entry.
      if (this.log.getAt(incoming.index) !== undefined) {
        this.logger.warn("Log conflict detected, truncating", {
          conflictIndex: incoming.index,
          localLastIndex: this.log.lastIndex
        });
        this.log.truncateTo(incoming.index - 1);
      }

      this.log.append(incoming);
      this.logger.info("Log entry appended", {
        index: incoming.index,
        strokeId: incoming.stroke.id
      });
    }

    // 6. Advance commitIndex as follower.
    this.maybeAdvanceCommitIndexAsFollower(payload.leaderCommit);

    return {
      term: this.currentTerm,
      success: true,
      matchIndex: this.log.lastIndex
    };
  }

  // ─── Member 2: sync-log (follower catch-up after restart) ─────────────────

  async handleSyncLog(payload: SyncLogRequest): Promise<SyncLogResponse> {
    // 1. Reject stale term.
    if (payload.term < this.currentTerm) {
      return {
        term: this.currentTerm,
        success: false,
        entries: [],
        reason: "Stale leader term"
      };
    }

    // 2. Step down / update leader if needed.
    if (payload.term > this.currentTerm || this.state !== "follower") {
      this.becomeFollower(payload.term, payload.leaderId);
    } else {
      this.leaderId = payload.leaderId;
      this.resetElectionTimer();
    }

    // 3. Return all entries from the requested index onward.
    const entries = this.log.getFrom(payload.fromIndex);

    this.logger.info("SyncLog responded", {
      fromIndex: payload.fromIndex,
      entryCount: entries.length
    });

    return {
      term: this.currentTerm,
      success: true,
      entries
    };
  }

  // ─── Member 2: leader stroke replication ──────────────────────────────────

  /**
   * Entry point for the gateway: only the elected leader should call this.
   *
   * 1. Appends the stroke to the leader's own log.
   * 2. Fans out append-entries to all peers in parallel.
   * 3. Counts acks; once a majority acknowledges, advances commitIndex.
   * 4. Returns the committed LogEntry.
   *
   * Throws if called on a non-leader or if a majority was not reached.
   */
  async replicateStroke(stroke: StrokeEvent): Promise<LogEntry> {
    if (this.state !== "leader") {
      throw new Error(`replicateStroke called on non-leader (state=${this.state})`);
    }

    // Append to own log.
    const entry: LogEntry = {
      term: this.currentTerm,
      index: this.log.length, // next available index
      stroke
    };
    this.log.append(entry);
    // Leader counts as 1 ack for itself.
    this.matchIndex.set(this.config.nodeId, entry.index);

    this.logger.info("Replicating stroke", {
      index: entry.index,
      strokeId: stroke.id,
      peers: this.config.peers
    });

    // Fan out to peers.
    const appendPayload: AppendEntriesRequest = {
      term: this.currentTerm,
      leaderId: this.config.nodeId,
      prevLogIndex: entry.index - 1,
      leaderCommit: this.commitIndex,
      entries: [entry]
    };

    const responses = await Promise.allSettled(
      this.config.peers.map((peer) =>
        this.postJson<AppendEntriesResponse>(`${peer}/append-entries`, appendPayload)
      )
    );

    // Tally acks and detect higher-term responses.
    for (let i = 0; i < responses.length; i++) {
      const result = responses[i];
      const peer = this.config.peers[i];

      if (result.status === "rejected") {
        this.logger.warn("AppendEntries to peer failed", { peer, error: String(result.reason) });
        continue;
      }

      const resp = result.value;

      if (resp.term > this.currentTerm) {
        // A higher term means we are a stale leader — step down.
        this.becomeFollower(resp.term, null);
        throw new Error("Lost leadership during replication (higher term seen)");
      }

      if (resp.success && resp.matchIndex !== undefined) {
        this.matchIndex.set(peer, resp.matchIndex);
        this.nextIndex.set(peer, resp.matchIndex + 1);
      }
    }

    // Advance commit index if a majority has the entry.
    this.tryAdvanceCommitIndex();

    if (this.commitIndex < entry.index) {
      throw new Error(
        `Majority not reached for index ${entry.index} (commitIndex=${this.commitIndex})`
      );
    }

    this.logger.info("Stroke committed", { index: entry.index, strokeId: stroke.id });
    return entry;
  }

  /**
   * Returns the committed portion of the log.
   * Used by GET /log.
   */
  getCommittedLog(): LogEntry[] {
    return this.log.getCommitted(this.commitIndex);
  }

  // ─── Member 1: election internals ────────────────────────────────────────

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

    // Member 2: initialise per-peer replication tracking.
    for (const peer of this.config.peers) {
      this.nextIndex.set(peer, this.log.length); // log.length = lastIndex + 1
      this.matchIndex.set(peer, -1);
    }
    // Leader tracks itself.
    this.matchIndex.set(this.config.nodeId, this.log.lastIndex);

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

  // ─── Member 2: commit helpers ─────────────────────────────────────────────

  /**
   * Leader-side commit rule (Raft §5.3 / §5.4).
   *
   * Finds the highest index N such that:
   *   - a majority of matchIndex values >= N, AND
   *   - the entry at N was written in the current term
   * then advances commitIndex to N.
   */
  private tryAdvanceCommitIndex(): void {
    const quorum = this.getQuorumSize();
    // Scan from the top of the log downward.
    for (let n = this.log.lastIndex; n > this.commitIndex; n--) {
      const entry = this.log.getAt(n);
      if (!entry) continue;

      // Only commit entries from the current term (Raft §5.4.2).
      if (entry.term !== this.currentTerm) continue;

      // Count how many nodes (including self) have index n.
      let ackCount = 0;
      for (const acked of this.matchIndex.values()) {
        if (acked >= n) ackCount += 1;
      }

      if (ackCount >= quorum) {
        this.commitIndex = n;
        this.logger.info("CommitIndex advanced", { commitIndex: n });
        break;
      }
    }
  }

  /**
   * Follower-side commit rule.
   *
   * Advances commitIndex to min(leaderCommit, log.lastIndex).
   */
  private maybeAdvanceCommitIndexAsFollower(leaderCommit: number): void {
    if (leaderCommit > this.commitIndex) {
      const newCommit = Math.min(leaderCommit, this.log.lastIndex);
      if (newCommit > this.commitIndex) {
        this.commitIndex = newCommit;
        this.logger.info("Follower commitIndex advanced", {
          commitIndex: this.commitIndex
        });
      }
    }
  }

  // ─── utilities ────────────────────────────────────────────────────────────

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
