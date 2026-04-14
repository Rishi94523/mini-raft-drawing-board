# Mini-RAFT Drawing Board

Starter codebase for the distributed real-time drawing board assignment.

## What is already done

- Shared TypeScript project scaffold
- Required top-level folders: `frontend`, `gateway`, `replica1`, `replica2`, `replica3`
- Member 1 starter implementation for RAFT node state, election timeout, vote handling, heartbeats, term updates, and leader promotion
- Replica HTTP endpoints for:
  - `POST /request-vote`
  - `POST /heartbeat`
  - `POST /append-entries`
  - `POST /sync-log`
  - `GET /status`
  - `GET /health`
- Initial Docker Compose draft for team integration
- Team ownership and status notes in `docs/TEAM_STATUS.md`

## Team ownership

- Member 1: RAFT election core
- Member 2: Log replication and recovery
- Member 3: Gateway and frontend
- Member 4: Docker, integration, testing, docs

Full details are in [docs/TEAM_STATUS.md](./docs/TEAM_STATUS.md).

## Local setup

```bash
npm install
npm run dev:replica1
```

Run the other replicas in separate terminals:

```bash
npm run dev:replica2
npm run dev:replica3
```

Optional gateway placeholder:

```bash
npm run dev:gateway
```

## Current project state

This repo is intentionally biased toward Member 1's area so work can begin in parallel:

- Replica election flow is scaffolded and testable.
- Replication endpoints exist but still need Member 2 implementation.
- Gateway and frontend are placeholders for Member 3.
- Docker Compose is a draft for Member 4 to refine.

## Quick checks

- `GET http://localhost:5001/status`
- `GET http://localhost:5002/status`
- `GET http://localhost:5003/status`

Each replica reports its node state, current term, vote, leader, and quorum target.
