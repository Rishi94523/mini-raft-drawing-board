# Team Split And Status

This project is being built in parallel over a short 2-3 day push, so ownership is strict and interfaces are frozen early.

## Member 1: RAFT election core

### Owns

- Node state machine: follower, candidate, leader
- Term tracking
- Election timeout: 500-800 ms
- RequestVote flow
- Heartbeat handling
- Majority quorum for leader election
- Dropping stale leaders and stepping down on higher terms

### Already done

- Shared RAFT types and replica config
- Replica server bootstrap for all three replicas
- Election timer and randomized timeout
- RequestVote RPC handler
- Heartbeat RPC handler
- Leader promotion and heartbeat loop
- Replica status endpoint for debugging

### Still to polish

- More test coverage around split votes
- Better logging around election retries
- Optional metrics endpoint if time allows

## Member 2: Replication and recovery

### Owns

- Append-only stroke log
- `POST /append-entries`
- Majority acknowledgement tracking for commits
- Commit index updates
- `POST /sync-log`
- Follower catch-up after restart
- Log ordering, correctness, and consistency checks

### Already done for Member 2

- Endpoint routes already exist
- Shared request and response types are defined
- Hooks are ready inside `RaftNode` for Member 2 to extend

### Member 2 next steps

- Implement real append logic
- Add commit index movement
- Add follower mismatch handling
- Add sync from leader starting at follower index

## Member 3: Gateway and frontend

### Owns

- Browser drawing UI
- Stroke payload format usage in the client
- WebSocket gateway
- Forwarding strokes to the current leader
- Broadcasting committed strokes to all connected clients
- Reconnect handling and multi-tab demo behavior

### Already done for Member 3

- Project folder exists
- Shared `StrokeEvent` type is defined
- Gateway placeholder server exists
- Leader status route placeholder exists

### Member 3 next steps

- Add WebSocket server with `ws`
- Add leader discovery in gateway
- Build canvas frontend
- Render local and remote strokes

## Member 4: Docker, integration, testing, docs

### Owns

- `docker-compose.yml`
- Healthchecks
- Bind mounts and hot reload
- Graceful shutdown and restart behavior
- End-to-end failure testing
- Demo script, architecture notes, final logs

### Already done for Member 4

- Initial compose draft is present
- Replica and gateway scripts are wired for local dev
- Health endpoints exist for container checks

### Member 4 next steps

- Replace draft container commands with final dev/prod flow
- Add bind-mounted hot reload tuning
- Add scripted failover testing
- Finalize architecture doc and demo notes

## Frozen contracts

- Replica APIs:
  - `POST /request-vote`
  - `POST /heartbeat`
  - `POST /append-entries`
  - `POST /sync-log`
  - `GET /status`
- Shared stroke shape:

```ts
{
  id: string;
  x0: number;
  y0: number;
  x1: number;
  y1: number;
  color: string;
  width: number;
  timestamp: number;
}
```

## Repo status summary

- Good starting point for Member 1 work: yes
- Good starting point for parallel team development: yes
- Production complete: no
- Ready for Member 2, 3, and 4 to branch and begin: yes
