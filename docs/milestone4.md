# Milestone 4: Coordination

Code: [`milestone4/`](../milestone4) · Tests: [`tests/test_milestone4.py`](../tests/test_milestone4.py)

| Requirement | Implementation | Where |
|---|---|---|
| Logical clocks | Lamport clock per node | `common.py` → `LamportClock` |
| Timestamps in messages | every protocol message has `ts` (plus `msg_id`) | `node.py` → `send()` / `handle()` |
| Leader election | Bully algorithm + heartbeat failure detector | `node.py` → `start_election`, `_monitor_loop` |
| Distributed mutual exclusion | Centralized mutex; coordinator = elected leader; queue ordered by Lamport timestamp | `node.py` → `_on_request`, `_try_grant`, `acquire`, `release` |
| Shared resource | Collaborative playlist file (`data/m4/shared_playlist.json`) + fencing tokens | `shared_playlist.py`, `node.py` → `_critical_section` |
| Event ordering | All node logs merged into one total order `(Lamport, node id)`, with automatic checks | `merge_logs.py` |

## 1. System setup

Five nodes (N1–N5), each a **separate OS process** listening on TCP `127.0.0.1:9101–9105`.
Messages use the same framing as Milestones 2/3: a 4-byte big-endian length header followed by a UTF-8 JSON body.
Each message is sent on its own TCP connection, and the receiver replies with a transport-level ack.
The ack piggybacks the receiver's election *term*, so a restarted node can catch up.

Message types: `HELLO`, `HEARTBEAT`, `ELECTION`, `OK`, `COORDINATOR`, `REQUEST`, `GRANT`, `RELEASE`, `STATE_REPORT`.

## 2. Logical-clock design

Each node owns one `LamportClock`, an integer protected by a lock because the server, the heartbeat monitor and the worker threads all touch it.

```json
{"type":"REQUEST","src":3,"dst":5,"ts":216,"msg_id":"N3.0-41","req_ts":190}
```

### Timestamp update rules

| Event | Rule | Example log line |
|---|---|---|
| Local event (e.g. `CS_REQUEST`, `CS_ENTER`, `LEADER_ELECTED`) | `C = C + 1` | `[L= 188] [N2] CS_REQUEST ... (req_ts=188)` |
| Send | `C = C + 1`; the message carries `ts = C` | `[L= 216] [N3] SEND REQUEST -> N4 (msg ts=216)` |
| Receive message with `ts` | `C = max(C, ts) + 1` | `[L= 219] [N4] RECV REQUEST <- N3 (msg ts=216, clock 218 -> max(218,216)+1 = 219)` |

- **Every receive is logged with the before and after values**, so the update rule is visible in the logs.
- **Heartbeats** also carry and update timestamps, but they are not printed. Heartbeats are sent every 0.5 s, so that would flood the console. As a result, you will see clock values jump between printed lines.
- **Crash recovery:** when a node restarts, it reads the highest Lamport value from its own log (stable storage) and resumes from there. A restarted node therefore never reissues timestamps it already used.

### Where the clock is used

1. **Mutex fairness.** A request's timestamp `req_ts` is the Lamport time at which the node decided it wanted the lock. The coordinator's waiting queue is a min-heap keyed on `(req_ts, node id)`, so waiting nodes are served in logical-time order. Ties are broken by node id.
2. **Event ordering.** `merge_logs.py` sorts every node's events by `(Lamport, node id)`. Lamport guarantees that if *a → b* (a happened-before b), then `L(a) < L(b)`. The node-id tie-break turns that partial order into a total order that is the same for everyone.
3. **Playlist entries** record the Lamport time of the write.

## 3. Leader election: Bully algorithm

The highest-numbered live node is the leader (the *coordinator*).

1. **Start.** A node starts an election when one of these happens:
   - it starts up, or restarts after a crash;
   - it sees no heartbeat from the leader for 1.5 s;
   - it fails to send a message to the leader;
   - it receives `ELECTION` from a lower node.
2. **Challenge.** It sends `ELECTION` to every node with a higher id.
3. **Answer.** A higher node that receives `ELECTION` replies `OK` (it "bullies" the lower node) and then runs its own election.
4. **Outcome.**
   - If no `OK` arrives within 0.8 s, the node declares itself leader and broadcasts `COORDINATOR(term)`.
   - If an `OK` does arrive, the node waits up to 3 s for a `COORDINATOR` message. If none comes, it restarts the election.
5. **Bully rule.** If a node receives `COORDINATOR` from a lower id, it rejects it (`BULLY` event) and starts its own election. The highest live id therefore always wins.
6. **Liveness.** The leader sends `HEARTBEAT` every 0.5 s. A follower that hears nothing for 1.5 s logs `LEADER_FAILED` and starts an election.

**Terms.** Every successful election increments a term number. Nodes learn the current term from:
- `COORDINATOR` messages, including rejected ones;
- the term piggybacked on acks;
- `HELLO` acks when a node restarts.

Terms are used to build fencing tokens (section 5).

## 4. Mutual-exclusion algorithm: centralized, coordinator = elected leader

**Requester (any node):**
```
CS_REQUEST (tick, remember req_ts)
REQUEST(req_ts) -> leader
wait for GRANT(token)
CS_ENTER ... edit shared playlist ... CS_EXIT
RELEASE(token) -> leader
```

**Coordinator (the current leader):**
- On `REQUEST`:
  - if the lock is free, send `GRANT` immediately;
  - otherwise push `(req_ts, node)` onto the min-heap and log `LOCK_QUEUED` with the whole queue.
- On `RELEASE`: pop the smallest `(req_ts, node)` and send it `GRANT` with a new fencing token `[term, seq]`.

**Properties:**
- **Safety:** at most one holder at a time.
- **Fairness:** waiting nodes are served in Lamport-timestamp order.
- **Message cost:** 3 messages per critical section (`REQUEST`, `GRANT`, `RELEASE`).

### Fault tolerance

| Failure | What happens |
|---|---|
| **Coordinator crashes** | 1. Followers detect the missing heartbeats and a Bully election picks the next-highest node.<br>2. The new coordinator does not know who holds the lock, so it enters a 1 s **recovery window**. During it, the lock holder sends `STATE_REPORT(holding, token)` and waiting nodes re-send `REQUEST` with their **original** `req_ts`, so they keep their place in line.<br>3. Once the window ends, the coordinator resumes granting. |
| **Lock holder crashes** | The coordinator's heartbeat send to the holder fails, so it logs `HOLDER_FAILED`, revokes the token and grants the lock to the next waiting node. |
| **Stale holder** (its lock came from a dead coordinator) | Prevented by fencing (section 5). |

## 5. Shared resource being protected

The resource is the **group's collaborative playlist**, a single JSON file (`data/m4/shared_playlist.json`) that every node updates with *read → modify → write*:

```json
{"name": "Group Road-Trip Mix", "version": 7, "last_token": [6, 1],
 "tracks": [{"title": "...", "added_by": "N1", "lamport": 243, "token": [6, 1]}, ...]}
```

**Why it needs coordination.** Without a lock, two nodes can both read version `v`, and both then write `v+1`. The second write silently erases the first track: a **lost update**. The `--unsafe` mode demonstrates this on a separate file (`unsafe_playlist.json`).

**Detection.** Inside the critical section a node re-reads the version before writing. If the version changed underneath it, the node logs `CONFLICT`.

**Fencing tokens (second line of defence).**
- Each `GRANT` carries a token `[term, seq]`, and tokens increase strictly across both grants and elections.
- The file stores `last_token`.
- A write whose token is not greater than `last_token` is rejected (`FENCED`). So even a node holding a stale lock from a dead coordinator cannot overwrite newer data.

## 6. Logs showing ordered events

`python3 -m milestone4.merge_logs --key --verify` prints the merged history plus automatic checks. Excerpt from a run where the coordinator crashed:

```
   #      L  node  wall clock    event            detail
 128     66  N5    23:08:38.133  LEADER_ELECTED   N5 is the coordinator (term 5); no higher node answered
 ...
         195 N5    23:08:46.602  LOCK_GRANTED     granting lock to N4 (smallest req_ts=190), fencing token=[5, 6]
         198 N5    23:08:46.602  LOCK_QUEUED      N1 (req_ts=187) must wait: lock held by N4; queue=[N1(ts=187), N2(ts=188)]
         201 N4    23:08:46.604  CS_ENTER         [safe] read playlist v5 (5 tracks), fencing token=[5, 6]
         201 N5    23:08:46.606  CRASH            crash requested by operator: exiting immediately
         189 N1    23:08:48.233  LEADER_FAILED    no heartbeat from coordinator N5 for 1.6s -> starting election
         211 N4    23:08:48.975  LEADER_ELECTED   N4 is the coordinator (term 6)
         220 N4    23:08:48.979  RECOVERED        N4 still holds the lock (token [5, 6]); it keeps it until RELEASE
         226 N4    23:08:49.107  LOCK_QUEUED      N3 (req_ts=189) must wait: lock held by N4; queue=[N1(ts=187), N2(ts=188), N3(ts=189)]
         235 N4    23:08:50.038  RECOVERY_DONE    lock holder=N4, waiting=[N1(ts=187), N2(ts=188), N3(ts=189)]
         236 N4    23:08:50.116  CS_EXIT          [safe] added 'Satellite Hearts (via N4)', wrote playlist v6
         240 N4    23:08:50.117  LOCK_GRANTED     granting lock to N1 (smallest req_ts=187), fencing token=[6, 1]
         243 N1    23:08:50.118  CS_ENTER         [safe] read playlist v6 (6 tracks), fencing token=[6, 1]

=== VERIFICATION ===
[causality]   83/83 send->receive pairs satisfy L(send) < L(receive)
[mutex:safe]  10 critical sections, 0 real-time overlaps, 0 Lamport-order interleavings
[mutex:unsafe] 5 critical sections, 4 real-time overlaps, 4 Lamport-order interleavings
[playlist]    10 committed writes, 10 tracks in file, version=10, tokens increasing: True
[unsafe file] 5 writes completed but only 1 tracks survived -> 4 LOST UPDATES without coordination
RESULT: PASS - no conflicting operations in protected mode
```

**What the excerpt shows:**
- **Lamport order is a valid total order.** Each `CS_EXIT` has a smaller Lamport value than the next `CS_ENTER`, because the chain `CS_EXIT → RELEASE → GRANT → CS_ENTER` is a happened-before chain.
- **Lamport order is not wall-clock order.** After the crash, N1's `LEADER_FAILED` (L=189) sorts *before* N5's `CRASH` (L=201) even though it happened 1.6 s later in real time. The two events are **concurrent**: no message links them. Lamport clocks only guarantee order for causally related events.

## 7. Failure test involving the coordinator

**Scenario** (Phase 4 of `python3 -m milestone4.demo`, also `test_coordinator_crash_reelection_and_recovery`):
1. N1–N4 request the lock at the same moment and each holds it for 3.5 s. The coordinator N5 grants one node and queues the other three.
2. N5 is killed with no goodbye messages.

**Observed:**
1. Followers log `LEADER_FAILED` after about 1.5 s without a heartbeat.
2. Bully election: N4 is elected for term *t+1*. The `ELECTION`/`OK` exchange is visible in the log.
3. N4 enters `RECOVERY`. The current holder reports `STATE_REPORT holding=True`, and the waiting nodes re-send `REQUEST` with their **original** `req_ts`.
4. N4 waits for the holder's `RELEASE`, then grants in Lamport order with new-term tokens `[t+1, 1]`, `[t+1, 2]`, and so on.
5. All writes are kept (9 tracks), with no `CONFLICT` and no overlapping critical sections.
6. **Leader replacement (Phase 5):** N5 restarts. It recovers its Lamport clock from its log, learns the current term from the `HELLO` acks, finds no higher node, and takes leadership back (term *t+2*). Edits continue through it.

**Additional test:** `test_lock_holder_crash_is_revoked`. The lock holder crashes, the coordinator detects it (`HOLDER_FAILED`) and grants the lock to the next waiting node.

## 8. Assumptions and limitations

- **Crash-stop failures** on a reliable local network. There are no network partitions and no Byzantine nodes.
- **Bully assumes timeouts are accurate.** A very slow leader could be wrongly declared dead. Fencing tokens keep the playlist safe even then.
- **The centralized coordinator is a bottleneck and a single point of failure.** This is mitigated by election plus state recovery, not removed.
