# Cross-Cluster Replication (CCR) Overview

## What is Cross-Cluster Replication?

Cross-Cluster Replication is an OpenSearch plugin that enables continuous, near real-time replication of indices from one OpenSearch cluster (leader) to another (follower). This enables disaster recovery, reduced query latency, horizontal scalability, and centralized reporting.

## Key Concepts

### Clusters
- **Leader Cluster**: The source cluster containing the original data
- **Follower Cluster**: The destination cluster that replicates data from the leader
- Must have cross-cluster connectivity established from follower → leader

### Replication Mode
- **Active-Passive**: The follower index is read-only during replication
- When replication stops, the follower index becomes writable (useful for failover)

### Indices
- **Leader Index**: The original index on the leader cluster
- **Follower Index**: The replicated index on the follower cluster (read-only during replication)

---

## Architecture Overview

### High-Level Flow

```
┌─────────────────┐                    ┌─────────────────┐
│ Leader Cluster  │                    │Follower Cluster │
│                 │                    │                 │
│  Leader Index   │◄───────────────────│  Follower API   │
│    (Primary)    │   Cross-cluster    │   (Initiates)   │
│                 │   connection       │                 │
└────────┬────────┘                    └────────┬────────┘
         │                                      │
         │ Continuous polling                   │
         │ for changes                          │
         │                                      │
         └──────────────────────────────────────┘
              Replicate operations
```

### Core Components

1. **ReplicationPlugin**
   - Main plugin entry point
   - Registers all REST APIs, transport actions, and tasks
   - Location: `src/main/kotlin/org/opensearch/replication/ReplicationPlugin.kt`

2. **Persistent Tasks**
   - Background tasks that survive node restarts
   - State stored in cluster state
   - Two types: Index-level and Cluster-level

3. **Repository Abstraction**
   - Uses snapshot/restore machinery for bootstrap
   - Exposes leader cluster as an internal repository
   - Location: `src/main/kotlin/org/opensearch/replication/repository/`

4. **ReplicationEngine**
   - Custom engine that intercepts operations
   - Marks replicated operations with Origin.REPLICA
   - Location: `src/main/kotlin/org/opensearch/replication/ReplicationEngine.kt`

---

## Replication Phases

### Phase 1: Bootstrap (Initial Sync)
1. **Retention Lease**: Follower adds retention lease on leader shards
   - Prevents translog truncation and soft delete merging
   - Preserves operation history needed for replication

2. **Snapshot & Restore**: 
   - Uses OpenSearch's snapshot/restore mechanism
   - Copies current index state from leader to follower
   - Creates follower index with same settings/mappings
   - If this fails, replication transitions to FAILED state

### Phase 2: Continuous Replication
Once bootstrap completes, shard-level replication begins:

1. **ShardReplicationTask** (one per primary shard)
   - Runs on same node as the follower's primary shard
   - Spawns reader and writer threads

2. **Replication Reader**
   - Long-polls leader shard for translog operations
   - Batches operations for efficiency
   - Multiple concurrent requests can be in flight
   - Default timeout: 5 minutes

3. **Replication Writer**
   - Reads operations from queue in order
   - Replays operations on follower shard
   - Fetches updated mappings from leader if needed
   - Updates retention lease after successful writes

---

## Task Hierarchy

```
IndexReplicationTask (per index)
├── Manages overall replication lifecycle
├── Runs bootstrap phase (snapshot/restore)
├── Monitors shard tasks
└── Spawns ShardReplicationTask for each primary shard
    ├── ShardReplicationTask (per primary shard)
    │   ├── Runs on same node as follower primary
    │   ├── Reader threads (poll leader)
    │   └── Writer threads (apply changes)

AutofollowTask (cluster-level)
└── Polls leader for indices matching patterns
    └── Automatically starts IndexReplicationTask for matches
```

### Index-Level Tasks

**IndexReplicationTask**
- Coordinates replication for entire index
- States: INIT → RESTORING → STARTED → FOLLOWING/FAILED
- Checkpoints progress to resume after interruption
- Location: `src/main/kotlin/org/opensearch/replication/task/`

**ShardReplicationTask**
- One per follower primary shard
- Co-located with primary shard (moves if shard relocates)
- Handles actual data replication
- Notifies index task on failures

### Cluster-Level Tasks

**AutofollowTask**
- Polls leader cluster periodically
- Starts replication for indices matching configured patterns
- Executes with user context from autofollow API call

---

## REST APIs

### 1. Start Replication
```bash
PUT ${FOLLOWER}/_plugins/_replication/<follower-index>/_start
{
  "leader_alias": "leader-cluster",
  "leader_index": "<leader-index>"
}
```
Initiates replication from leader index to follower index.

### 2. Stop Replication
```bash
POST ${FOLLOWER}/_plugins/_replication/<follower-index>/_stop
{}
```
Stops replication and makes follower index writable (for failover).

### 3. Start Autofollow
```bash
POST ${FOLLOWER}/_plugins/_replication/_autofollow
{
  "leader_alias": "leader-cluster",
  "pattern": "logs-*",
  "name": "my-autofollow-rule"
}
```
Automatically replicates indices matching pattern.

### 4. Remove Autofollow
```bash
DELETE ${FOLLOWER}/_plugins/_replication/_autofollow
{
  "leader_alias": "leader-cluster",
  "name": "my-autofollow-rule"
}
```
Stops creating new replications (doesn't stop existing ones).

### 5. Check Autofollow Stats
```bash
GET ${FOLLOWER}/_plugins/_replication/autofollow_stats
```

### 6. Check Tasks
```bash
GET ${FOLLOWER}/_cat/tasks?v&actions=*replication*&detailed
```

---

## Key Design Principles

1. **Security**: Strong security controls via OpenSearch Security plugin integration
   - Node-to-node encryption
   - User context preservation for API calls
   - Permissions model for leader/follower

2. **Correctness**: No difference between leader and follower contents
   - Ordered replay of operations
   - Mapping updates synchronized

3. **Performance**: Minimal impact on leader cluster indexing rate
   - Asynchronous replication
   - Batching of operations

4. **Low Lag**: Replication lag under a few seconds
   - Long-polling for immediate updates
   - Multiple concurrent reader threads

5. **Minimal Resources**: Efficient resource usage
   - Per-shard granularity
   - Retention lease cleanup

---

## Source Code Structure

```
src/main/kotlin/org/opensearch/replication/
├── ReplicationPlugin.kt          # Main plugin entry
├── ReplicationEngine.kt           # Custom engine for replicated indices
├── ReplicationSettings.kt         # Configuration settings
├── action/                        # REST APIs and transport actions
│   ├── changes/                   # Fetch changes from leader
│   ├── index/                     # Start/stop replication
│   ├── replicationstatedetails/   # Get replication status
│   ├── replay/                    # Replay operations
│   ├── repository/                # Bootstrap via snapshot/restore
│   └── status/                    # Status APIs
├── metadata/                      # Metadata management
│   ├── ReplicationMetadataManager.kt
│   └── UpdateMetadataAction.kt
├── repository/                    # Bootstrap/restore logic
│   ├── RemoteClusterRepository.kt
│   └── RemoteClusterRestoreLeaderService.kt
├── seqno/                        # Sequence numbers and retention leases
│   ├── RemoteClusterRetentionLeaseHelper.kt
│   └── RemoteClusterTranslogService.kt
├── task/                         # Persistent tasks
│   ├── CrossClusterReplicationTask.kt
│   ├── ReplicationState.kt
│   └── index/                    # Index and shard tasks
└── util/                         # Utilities
    ├── Extensions.kt
    ├── SecurityContext.kt
    └── ValidationUtil.kt
```

---

## Use Cases

1. **Disaster Recovery (DR) / High Availability (HA)**
   - Maintain hot standby cluster
   - Failover by stopping replication (makes follower writable)

2. **Reduced Query Latency**
   - Replicate to clusters near users
   - Route queries to nearest cluster

3. **Scaling Query Workloads**
   - Distribute read queries across multiple followers
   - Leader handles writes, followers handle reads

4. **Aggregated Reporting**
   - Replicate from regional clusters to central reporting cluster
   - Consolidate dashboards and visualizations

---

## Prerequisites

1. **Cross-Cluster Connectivity**: Follower must have remote connection to leader
   ```bash
   PUT ${FOLLOWER}/_cluster/settings
   {
     "persistent": {
       "cluster.remote.leader-cluster.seeds": ["<leader-ip>:9300"]
     }
   }
   ```

2. **Plugin Installation**: CCR plugin on all nodes of both clusters

3. **Remote Cluster Client Role**: All follower nodes need this role

4. **Security Configuration** (if using Security plugin):
   - User injection enabled on both clusters
   - Nodes DN configured on leader to accept follower connections
   - Appropriate permissions on both clusters

---

## Monitoring & Debugging

### Task Status
- Check `.tasks` index for completed/failed tasks
- Contains failure reasons for debugging

### Logs
- Recent commit added diagnostic logging for replication failures
- Check node logs for detailed error messages

### Stats
- Use autofollow_stats API for autofollow health
- Monitor replication lag via task status

---

## Recent Changes (from git log)

1. **Diagnostic Logging** (commit e39a023)
   - Added logging to identify CCR replication failures

2. **API Updates** (commit d864481)
   - Removed deprecated `master_timeout`
   - Updated to `cluster_manager_timeout`

3. **Security Fixes** (commit 2af49c1)
   - Fixed CVE-2026-25645 and CVE-2026-24400

4. **Integration Test Fixes** (commit 515abbc)
   - ReplicationEngine creates lightweight operation copies
   - Marks operations with Origin.REPLICA before non-primary planning

---

## Next Steps for Learning

1. **Read the RFC**: `docs/RFC.md` has detailed architecture diagrams
2. **Try the Quick Start**: Follow README.md steps to run locally
3. **Explore Code**:
   - Start with `ReplicationPlugin.kt` to see all registered components
   - Look at `action/index/` for start/stop replication logic
   - Check `task/index/` for task implementation
4. **Run Integration Tests**: `./gradlew integTest`
5. **Check HANDBOOK.md**: Detailed API examples with security setup

---

## Key Files to Understand

| File | Purpose |
|------|---------|
| `ReplicationPlugin.kt` | Plugin registration, wires everything together |
| `ReplicationEngine.kt` | Custom engine for replicated shards |
| `action/index/TransportStartReplicationAction.kt` | Start replication logic |
| `action/index/TransportStopReplicationAction.kt` | Stop replication logic |
| `task/index/IndexReplicationTask.kt` | Index-level task orchestration |
| `task/shard/ShardReplicationTask.kt` | Shard-level replication worker |
| `repository/RemoteClusterRepository.kt` | Bootstrap via snapshot/restore |
| `seqno/RemoteClusterTranslogService.kt` | Fetch changes from leader |

---

## Common Questions

**Q: Can I write to the follower index?**
A: No, follower indices are read-only during replication. Stop replication first.

**Q: What happens if the leader goes down?**
A: Replication pauses. Stop replication on follower and promote it to writable.

**Q: Can I replicate to multiple followers?**
A: Yes, each follower independently replicates from the leader.

**Q: Does replication impact leader performance?**
A: Minimal impact - replication is asynchronous and doesn't block writes.

**Q: What's the typical replication lag?**
A: Under a few seconds during normal operation.

**Q: Can I filter which documents are replicated?**
A: No, entire index is replicated. Use different indices for different data.
