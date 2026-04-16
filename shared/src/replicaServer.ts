import express from "express";
import http from "node:http";
import { loadReplicaConfig, ReplicaDefaults } from "./config.js";
import { Logger } from "./logger.js";
import { RaftNode } from "./raftNode.js";
import {
  AppendEntriesRequest,
  HeartbeatRequest,
  RequestVoteRequest,
  StrokeEvent,
  SyncLogRequest
} from "./types.js";

export function startReplicaServer(defaults: ReplicaDefaults): void {
  const config = loadReplicaConfig(defaults);
  const logger = new Logger(config.nodeId);
  const raftNode = new RaftNode(config, logger);
  const app = express();

  app.use(express.json());

  // ── health / status ────────────────────────────────────────────────────────

  app.get("/health", (_req, res) => {
    res.json({ ok: true, nodeId: config.nodeId });
  });

  app.get("/status", (_req, res) => {
    res.json(raftNode.getSnapshot());
  });

  // ── Member 1: election RPCs ────────────────────────────────────────────────

  app.post("/request-vote", async (req, res) => {
    const result = await raftNode.handleRequestVote(req.body as RequestVoteRequest);
    res.json(result);
  });

  app.post("/heartbeat", async (req, res) => {
    const result = await raftNode.handleHeartbeat(req.body as HeartbeatRequest);
    res.json(result);
  });

  // ── Member 2: replication RPCs ────────────────────────────────────────────

  app.post("/append-entries", async (req, res) => {
    const result = await raftNode.handleAppendEntries(
      req.body as AppendEntriesRequest
    );
    res.json(result);
  });

  app.post("/sync-log", async (req, res) => {
    const result = await raftNode.handleSyncLog(req.body as SyncLogRequest);
    res.json(result);
  });

  /**
   * POST /stroke
   *
   * Body: StrokeEvent (the frozen shape from TEAM_STATUS.md).
   *
   * Leader-only endpoint.  The gateway (Member 3) calls this to submit a new
   * stroke.  The leader appends it to its log, replicates to peers, waits for
   * majority ack, and responds with the committed LogEntry.
   *
   * Non-leaders respond 403 so the gateway can redirect to the current leader.
   */
  app.post("/stroke", async (req, res) => {
    const snapshot = raftNode.getSnapshot();
    if (snapshot.state !== "leader") {
      res.status(403).json({
        error: "not-leader",
        leaderId: snapshot.leaderId
      });
      return;
    }

    try {
      const stroke = req.body as StrokeEvent;
      const committed = await raftNode.replicateStroke(stroke);
      res.json({ success: true, entry: committed });
    } catch (err) {
      logger.error("Stroke replication failed", err);
      res.status(500).json({ error: String(err) });
    }
  });

  /**
   * GET /log
   *
   * Returns the committed portion of this replica's stroke log.
   * Useful for debugging, catch-up inspection, and integration tests.
   */
  app.get("/log", (_req, res) => {
    res.json({
      nodeId: config.nodeId,
      commitIndex: raftNode.getSnapshot().commitIndex,
      entries: raftNode.getCommittedLog()
    });
  });

  // ── server bootstrap ──────────────────────────────────────────────────────

  const server = http.createServer(app);

  server.listen(config.port, () => {
    logger.info("Replica server listening", {
      port: config.port
    });
    raftNode.start();
  });

  const shutdown = () => {
    logger.info("Replica server shutting down");
    raftNode.stop();
    server.close(() => {
      process.exit(0);
    });
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
