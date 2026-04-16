import express from "express";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer, WebSocket } from "ws";
import { StrokeEvent, ReplicaSnapshot } from "../../shared/src/types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(express.json());

// Serve static frontend files
const publicPath = path.resolve(__dirname, "../../frontend/public");
const distPath = path.resolve(__dirname, "../../dist");
app.use(express.static(publicPath));
app.use("/dist", express.static(distPath));

const REPLICAS = (process.env.REPLICAS ?? "http://replica1:5001,http://replica2:5002,http://replica3:5003").split(",");

let currentLeaderUrl: string | null = null;

async function discoverLeader() {
  for (const url of REPLICAS) {
    try {
      const resp = await fetch(`${url}/status`);
      if (resp.ok) {
        const status = (await resp.json()) as ReplicaSnapshot;
        if (status.state === "leader") {
          currentLeaderUrl = url;
          return url;
        }
      }
    } catch (err) {
      // Replica might be down, ignore
    }
  }
  return null;
}

// Initial discovery
discoverLeader().then(leader => {
  if (leader) console.log(`[gateway] Initial leader found: ${leader}`);
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "gateway",
    leader: currentLeaderUrl,
    replicas: REPLICAS
  });
});

app.get("/leader", async (_req, res) => {
  const leader = await discoverLeader();
  if (leader) {
    res.json({ leader });
  } else {
    res.status(503).json({ error: "No leader found in cluster" });
  }
});

app.get("/log", async (_req, res) => {
  // Try to get log from leader, or any replica
  const urls = currentLeaderUrl ? [currentLeaderUrl, ...REPLICAS] : REPLICAS;
  for (const url of urls) {
    try {
      const resp = await fetch(`${url}/log`);
      if (resp.ok) {
        const data = await resp.json();
        return res.json(data);
      }
    } catch (err) {}
  }
  res.status(503).json({ error: "Could not fetch log from cluster" });
});

const port = Number(process.env.GATEWAY_PORT ?? 4000);
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const clients = new Set<WebSocket>();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`[gateway] Client connected. Total: ${clients.size}`);

  ws.on("message", async (data) => {
    try {
      const message = JSON.parse(data.toString());
      if (message.type === "stroke") {
        const stroke = message.data as StrokeEvent;
        
        // Forward to leader
        let leader = currentLeaderUrl || (await discoverLeader());
        if (!leader) {
          ws.send(JSON.stringify({ type: "error", message: "No leader available" }));
          return;
        }

        try {
          const resp = await fetch(`${leader}/stroke`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(stroke)
          });

          if (resp.ok) {
            const result = await resp.json();
            // Broadcast committed stroke to ALL clients (including sender)
            broadcast({ type: "stroke-committed", data: result.entry });
          } else if (resp.status === 403) {
            // Leader changed
            console.log("[gateway] 403 from replica, re-discovering leader...");
            currentLeaderUrl = null;
            const newLeader = await discoverLeader();
            if (newLeader) {
              // Retry once
              const retryResp = await fetch(`${newLeader}/stroke`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(stroke)
              });
              if (retryResp.ok) {
                const result = await retryResp.json();
                broadcast({ type: "stroke-committed", data: result.entry });
              }
            }
          }
        } catch (err) {
          console.error("[gateway] Error forwarding stroke:", err);
          currentLeaderUrl = null; // Reset leader cache on error
        }
      }
    } catch (err) {
      console.error("[gateway] Error processing message:", err);
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[gateway] Client disconnected. Total: ${clients.size}`);
  });
});

function broadcast(message: any) {
  const payload = JSON.stringify(message);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

server.listen(port, () => {
  console.log(`[gateway] listening on ${port}`);
});
