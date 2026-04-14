# API Contracts

These contracts are the shared interface between Member 1, 2, and 3 so the project can move in parallel.

## RequestVote

`POST /request-vote`

```json
{
  "term": 3,
  "candidateId": "replica2"
}
```

Response:

```json
{
  "term": 3,
  "voteGranted": true
}
```

## Heartbeat

`POST /heartbeat`

```json
{
  "term": 3,
  "leaderId": "replica2"
}
```

Response:

```json
{
  "term": 3,
  "success": true
}
```

## AppendEntries

`POST /append-entries`

```json
{
  "term": 3,
  "leaderId": "replica2",
  "prevLogIndex": 4,
  "leaderCommit": 4,
  "entries": [
    {
      "term": 3,
      "index": 5,
      "stroke": {
        "id": "stroke-5",
        "x0": 10,
        "y0": 12,
        "x1": 44,
        "y1": 55,
        "color": "#111111",
        "width": 3,
        "timestamp": 1712850000
      }
    }
  ]
}
```

Response:

```json
{
  "term": 3,
  "success": true,
  "matchIndex": 5
}
```

## SyncLog

`POST /sync-log`

```json
{
  "term": 3,
  "leaderId": "replica2",
  "fromIndex": 4
}
```

Response:

```json
{
  "term": 3,
  "success": true,
  "entries": []
}
```

## Gateway to frontend WebSocket event shape

```json
{
  "type": "stroke-committed",
  "payload": {
    "id": "stroke-5",
    "x0": 10,
    "y0": 12,
    "x1": 44,
    "y1": 55,
    "color": "#111111",
    "width": 3,
    "timestamp": 1712850000
  }
}
```
