import express from "express";
import http from "node:http";

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "gateway",
    status: "placeholder",
    owner: "Member 3"
  });
});

app.get("/leader", (_req, res) => {
  res.status(501).json({
    message: "Leader discovery is pending Member 3 implementation."
  });
});

const port = Number(process.env.GATEWAY_PORT ?? 4000);
const server = http.createServer(app);

server.listen(port, () => {
  console.log(`[gateway] listening on ${port}`);
});
