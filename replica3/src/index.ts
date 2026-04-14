import { startReplicaServer } from "../../shared/src/replicaServer.js";

startReplicaServer({
  nodeId: "replica3",
  port: 5003,
  peers: ["http://localhost:5001", "http://localhost:5002"]
});
