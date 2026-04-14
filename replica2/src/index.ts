import { startReplicaServer } from "../../shared/src/replicaServer.js";

startReplicaServer({
  nodeId: "replica2",
  port: 5002,
  peers: ["http://localhost:5001", "http://localhost:5003"]
});
