import express from "express";
import http from "node:http";
import { loadReplicaConfig, ReplicaDefaults } from "./config.js";
import { Logger } from "./logger.js";
import { RaftNode } from "./raftNode.js";
import {
  AppendEntriesRequest,
  HeartbeatRequest,
  RequestVoteRequest,
  SyncLogRequest
} from "./types.js";

export function startReplicaServer(defaults: ReplicaDefaults): void {
  const config = loadReplicaConfig(defaults);
  const logger = new Logger(config.nodeId);
  const raftNode = new RaftNode(config, logger);
  const app = express();

  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.json({ ok: true, nodeId: config.nodeId });
  });

  app.get("/status", (_req, res) => {
    res.json(raftNode.getSnapshot());
  });

  app.post("/request-vote", async (req, res) => {
    const result = await raftNode.handleRequestVote(req.body as RequestVoteRequest);
    res.json(result);
  });

  app.post("/heartbeat", async (req, res) => {
    const result = await raftNode.handleHeartbeat(req.body as HeartbeatRequest);
    res.json(result);
  });

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
