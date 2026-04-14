import { startReplicaServer } from "../../shared/src/replicaServer.js";

startReplicaServer({
  nodeId: "replica1",
  port: 5001,
  peers: ["http://localhost:5002", "http://localhost:5003"]
});
