# Architecture

## Full system design

The target system is a hybrid architecture: a logically centralized control
plane (gateway, user, catalog, playlist, playback, recommendation services)
that is physically distributed across independent services, plus a
decentralized P2P layer for audio content distribution.

```mermaid
flowchart TB
    Client["Client (web / desktop / mobile / CLI)"]

    subgraph ControlPlane["Control Plane (this milestone)"]
        Gateway["API Gateway :8000"]
        UserSvc["User Service :8001"]
        PlaylistSvc["Playlist Service :8002"]
        CatalogSvc["Catalog Service :8003"]
    end

    subgraph Planned["Planned (later milestones)"]
        PlaybackSvc["Playback Session Service"]
        RecoSvc["Recommendation Service"]
        Peers["Peer nodes (cached audio chunks)"]
    end

    Client -->|"HTTP request<br/>X-Request-ID"| Gateway
    Gateway -->|"GET /users/{id}<br/>validate user"| UserSvc
    Gateway -->|"GET/POST /playlists<br/>fetch or create"| PlaylistSvc
    Gateway -->|"GET /tracks/{id}"| CatalogSvc
    PlaylistSvc -->|"GET /tracks?ids=...<br/>hydrate track names"| CatalogSvc

    Gateway -.->|planned| PlaybackSvc
    PlaybackSvc -.->|planned| RecoSvc
    Client -.->|planned P2P chunk sharing| Peers
    Peers -.-> Peers

    style ControlPlane fill:#e8f0fe,stroke:#4285f4
    style Planned fill:#f5f5f5,stroke:#999,stroke-dasharray: 4 4
```

## Milestone 1 request flow

The centerpiece demo is `GET /users/42/playlists`, which fans out across
all three backend services under a single correlation ID:

```mermaid
sequenceDiagram
    participant C as Client
    participant G as Gateway :8000
    participant U as User Service :8001
    participant P as Playlist Service :8002
    participant Cat as Catalog Service :8003

    C->>G: GET /users/42/playlists (X-Request-ID: r1)
    G->>U: GET /users/42 (X-Request-ID: r1)
    U-->>G: 200 {id, username, tier}
    G->>P: GET /playlists?owner_id=42 (X-Request-ID: r1)
    P->>Cat: GET /tracks?ids=1,3,5,... (X-Request-ID: r1)
    Cat-->>P: 200 {tracks: [...]}
    P-->>G: 200 {playlists: [...], degraded: false}
    G-->>C: 200 combined response
```

Every hop logs a JSON line tagged with `request_id=r1`, so
`scripts/trace.py r1` reconstructs this exact sequence from the log files.

## Components (Milestone 1)

| Service | Responsibility | Depends on |
|---|---|---|
| API Gateway | Single client entry point; request ID assignment; routing; logging | User, Playlist, Catalog |
| User Service | Account lookup (id, username, subscription tier) | none |
| Playlist Service | Owns playlists; hydrates track metadata | Catalog |
| Catalog Service | Track metadata | none |

## System-model assumptions

- **Organization:** hybrid — the control plane above is logically
  centralized but physically distributed; a P2P content layer is planned
  for a later milestone.
- **Timing:** semi-synchronous. Client-to-service and service-to-service
  calls are synchronous request/response with bounded timeouts (2s) used
  for failure detection.
- **Node failures:** crash-stop / crash-recovery. No Byzantine failures.
  Each service owns its own in-memory (later: persistent) store.
- **Link failures:** messages can be lost, delayed, duplicated, or
  reordered. Handled with timeouts and limited retries on idempotent (GET)
  requests only.
- **Storage:** each service owns its own store; no shared database. Audio
  files (planned) are immutable; playlists and sessions are mutable shared
  resources, which is why they are the target for later coordination and
  replication milestones.

## Planned services (future milestones)

- **Playback Session Service** — tracks the current track/position per
  user and enforces "one active device per account."
- **Recommendation Service** — consumes play events asynchronously;
  degrades gracefully if unavailable.
- **Content / Peer layer** — audio chunks served from storage nodes, with
  clients acting as peers that cache and share chunks (P2P integration).
