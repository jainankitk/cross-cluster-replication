# Retention Lease Flow Diagram

## Complete Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RETENTION LEASE LIFECYCLE                        │
└─────────────────────────────────────────────────────────────────────────┘

                            FOLLOWER CLUSTER
                                  │
                                  │ 1. Start Replication API
                                  ▼
                         IndexReplicationTask
                                  │
                                  │ 2. setupAndStartRestore()
                                  ▼
                    ┌──────────────────────────────┐
                    │   LEADER CLUSTER: Bootstrap  │
                    ├──────────────────────────────┤
                    │ 1. acquireHistoryRetentionLock() │
                    │ 2. acquireSafeIndexCommit()      │
                    │                                   │
                    │ 3. ADD RETENTION LEASE            │
                    │    retainingSeqNo = RETAIN_ALL    │
                    │    (preserve EVERYTHING)          │
                    │                                   │
                    │ 4. releaseRetentionLock()         │
                    └──────────────────────────────┘
                                  │
                                  │ 3. Snapshot/Restore
                                  ▼
                         ┌─────────────────┐
                         │ Follower Index  │
                         │   Created       │
                         └─────────────────┘
                                  │
                                  │ 4. Spawn ShardReplicationTasks
                                  ▼
                    ┌──────────────────────────────┐
                    │ ShardReplicationTask Starts  │
                    ├──────────────────────────────┤
                    │ RENEW RETENTION LEASE         │
                    │ Old: RETAIN_ALL (-1)          │
                    │ New: globalCheckpoint + 1     │
                    │                               │
                    │ Example: seqNo = 1001         │
                    └──────────────────────────────┘
                                  │
                                  │ 5. Continuous Replication
                                  ▼
        ┌───────────────────────────────────────────────────┐
        │          REPLICATION LOOP (per shard)             │
        ├───────────────────────────────────────────────────┤
        │                                                   │
        │  ┌─────────────────────────────────────────┐     │
        │  │ READER: Poll leader for changes          │     │
        │  │   getChanges(fromSeqNo, toSeqNo)         │     │
        │  │   ├─> Get batch of operations            │     │
        │  │   └─> Push to sequencer queue            │     │
        │  └─────────────────────────────────────────┘     │
        │                    │                              │
        │                    ▼                              │
        │  ┌─────────────────────────────────────────┐     │
        │  │ WRITER: Apply changes to follower        │     │
        │  │   ├─> Read from queue                    │     │
        │  │   ├─> Replay operations                  │     │
        │  │   ├─> Update mappings if needed          │     │
        │  │   └─> Write to follower shard            │     │
        │  └─────────────────────────────────────────┘     │
        │                    │                              │
        │                    ▼                              │
        │  ┌─────────────────────────────────────────┐     │
        │  │ RENEW RETENTION LEASE                    │     │
        │  │   retainingSeqNo =                       │     │
        │  │     globalCheckpoint + 1                 │     │
        │  │                                          │     │
        │  │   Leader can now cleanup ops < seqNo    │     │
        │  └─────────────────────────────────────────┘     │
        │                    │                              │
        │                    └──────┐                       │
        │                           │ Loop continues        │
        │                           └────────────────────┐  │
        │                                                │  │
        └────────────────────────────────────────────────┘  │
                                                             │
                              ┌──────────────────────────────┘
                              │
                              │ 6. Stop Replication API
                              ▼
                    ┌──────────────────────────────┐
                    │   CLEANUP: Stop Replication  │
                    ├──────────────────────────────┤
                    │ 1. Cancel shard tasks         │
                    │ 2. REMOVE RETENTION LEASE     │
                    │    from leader shards         │
                    │ 3. Follower index -> writable │
                    └──────────────────────────────┘
```

---

## Retention Lease Protection Mechanism

```
┌─────────────────────────────────────────────────────────────────────────┐
│          HOW RETENTION LEASES PREVENT OPERATIONS DELETION               │
└─────────────────────────────────────────────────────────────────────────┘

                              LEADER SHARD
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │   TRANSLOG         │      │   LUCENE SEGMENTS      │
        │   (Operations)     │      │   (Soft Deletes)       │
        └────────────────────┘      └────────────────────────┘
                    │                             │
                    │                             │
        Want to truncate old files    Want to merge away deletes
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │ Check Deletion     │      │ Check Merge Policy      │
        │ Policy             │      │                         │
        └────────────────────┘      └────────────────────────┘
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │ Query ALL          │      │ Query ALL              │
        │ Retention Leases   │      │ Retention Leases       │
        └────────────────────┘      └────────────────────────┘
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │ Find MIN retaining │      │ Find MIN retaining     │
        │ sequence number    │      │ sequence number        │
        │                    │      │                        │
        │ Example: seqNo=1500│      │ Example: seqNo=1500    │
        └────────────────────┘      └────────────────────────┘
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │ Find translog gen  │      │ Preserve segments      │
        │ containing seqNo   │      │ with seqNo >= 1500     │
        │ 1500               │      │                        │
        │                    │      │ Skip merging soft      │
        │ Keep that gen and  │      │ deletes in range       │
        │ all newer gens     │      │                        │
        └────────────────────┘      └────────────────────────┘
                    │                             │
                    ▼                             ▼
        ┌────────────────────┐      ┌────────────────────────┐
        │ Can delete older   │      │ Can merge segments     │
        │ generations        │      │ without needed data    │
        └────────────────────┘      └────────────────────────┘


    ┌───────────────────────────────────────────────────────────┐
    │  RESULT: Operations needed for replication are preserved  │
    │                                                           │
    │  • Follower can always fetch operations from leader       │
    │  • No gaps in sequence number continuity                 │
    │  • Enables catch-up after temporary disconnection        │
    └───────────────────────────────────────────────────────────┘
```

---

## Retention Lease Value Progression

```
┌─────────────────────────────────────────────────────────────────────────┐
│              RETENTION LEASE VALUE OVER TIME                            │
└─────────────────────────────────────────────────────────────────────────┘

PHASE 1: BOOTSTRAP
─────────────────────────────────────────────────────────────────────
Time: T0 (Start replication)

Retention Lease: retainingSeqNo = RETAIN_ALL (-1)
Leader SeqNo:    [0 ─────────────────────────────────────> 5000]
                 │                                              │
                 └──────────────────────────────────────────────┘
                          ALL preserved (bootstrap in progress)


PHASE 2: INITIAL RENEWAL
─────────────────────────────────────────────────────────────────────
Time: T1 (Bootstrap complete, shard task starts)

Follower state: globalCheckpoint = 4999 (caught up to snapshot)

Retention Lease: retainingSeqNo = 5000 (globalCheckpoint + 1)
Leader SeqNo:    [0 ─────────────────────> 4999][5000 ──────> 5200]
                 │                              │              │
                 └──────────────────────────────┘              │
                        Can be deleted            Preserved


PHASE 3: CONTINUOUS REPLICATION
─────────────────────────────────────────────────────────────────────
Time: T2 (Replication progressing)

Follower state: globalCheckpoint = 5150

Retention Lease: retainingSeqNo = 5151 (renewed)
Leader SeqNo:    [0 ───────> 5150][5151 ──────────────────> 5500]
                 │               │                            │
                 └───────────────┘                            │
                  Can be deleted              Preserved


Time: T3 (More progress)

Follower state: globalCheckpoint = 5480

Retention Lease: retainingSeqNo = 5481 (renewed)
Leader SeqNo:    [0 ──────────────> 5480][5481 ──────> 5600]
                 │                       │              │
                 └───────────────────────┘              │
                      Can be deleted        Preserved


PHASE 4: STOP REPLICATION
─────────────────────────────────────────────────────────────────────
Time: T4 (Replication stopped)

Retention Lease: REMOVED

Leader SeqNo:    [0 ──────────────────────────────────────> 6000]
                 │                                             │
                 └─────────────────────────────────────────────┘
                  Cleanup based on other policies (age, size, etc.)
```

---

## Error Scenarios

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   RETENTION LEASE ERROR HANDLING                        │
└─────────────────────────────────────────────────────────────────────────┘


ERROR 1: Retention Lease Already Exists
─────────────────────────────────────────────────────────────────────

    Follower                    Leader
       │                          │
       │  Add Retention Lease     │
       ├─────────────────────────>│
       │                          │ Check: Lease exists?
       │                          │ Yes! (stale from old replication)
       │                          │
       │  AlreadyExistsException  │
       │<─────────────────────────┤
       │                          │
       │  Remove Stale Lease      │
       ├─────────────────────────>│
       │                          │ Removed
       │         OK               │
       │<─────────────────────────┤
       │                          │
       │  Retry: Add Lease        │
       ├─────────────────────────>│
       │                          │ Success!
       │         OK               │
       │<─────────────────────────┤


ERROR 2: Invalid Retaining SeqNo
─────────────────────────────────────────────────────────────────────

    Follower                    Leader
       │                          │
       │  Current Lease: 5000     │
       │                          │
       │  Renew with seqNo: 4900  │
       ├─────────────────────────>│
       │                          │ Check: 4900 < 5000?
       │                          │ Yes! Invalid!
       │                          │
       │  InvalidSeqNoException   │
       │<─────────────────────────┤
       │                          │
       │  Check last renewal time │
       │  < 10 minutes? → Log     │
       │  > 10 minutes? → FAIL    │


ERROR 3: Lease Not Found
─────────────────────────────────────────────────────────────────────

    Follower                    Leader
       │                          │
       │  Renew Retention Lease   │
       ├─────────────────────────>│
       │                          │ Search for lease
       │                          │ Not found!
       │                          │
       │  NotFoundException       │
       │<─────────────────────────┤
       │                          │
       │  Check old lease format  │
       │  (cluster name without   │
       │   UUID)                  │
       │                          │
       │  Found? → Add new lease  │
       │  Not found? → FAIL       │


ERROR 4: Operations Already Deleted
─────────────────────────────────────────────────────────────────────

    Timeline:
    
    T0: Replication starts, lease should be added
    T1: Network partition - lease not added
    T2: Leader truncates translog (no lease to stop it)
    T3: Network heals, follower tries to add lease
    
    Result: Operations 1000-2000 already deleted
    
    Solution: FULL RE-BOOTSTRAP required
              ├─> Stop replication
              ├─> Clear follower index
              └─> Start replication (new snapshot)
```

---

## Monitoring Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    RETENTION LEASE MONITORING                           │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Leader Cluster - Retention Lease Status                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Shard: leader-01[0]                                                 │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Retention Leases:                                            │    │
│  │                                                              │    │
│  │  • replication:follower-cluster:uuid:[follower-01][0]       │    │
│  │    Retaining SeqNo: 5481                                    │    │
│  │    Last Updated: 2026-05-01 10:30:15                       │    │
│  │                                                              │    │
│  │  • replication:backup-cluster:uuid:[backup-01][0]           │    │
│  │    Retaining SeqNo: 5200                                    │    │
│  │    Last Updated: 2026-05-01 10:30:10                       │    │
│  │                                                              │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  Translog:                                                           │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Current SeqNo: 5600                                         │    │
│  │  Operations:    400  (range: 5200-5600)                     │    │
│  │  Size:          2.5 MB                                      │    │
│  │  Generations:   3                                           │    │
│  │                                                              │    │
│  │  Oldest Retained SeqNo: 5200 (due to backup-cluster lease) │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Follower Cluster - Replication Progress                             │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Index: follower-01                                                  │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Shard [0]:                                                   │    │
│  │                                                              │    │
│  │  Local Checkpoint:        5480                              │    │
│  │  Global Checkpoint:       5480                              │    │
│  │  Leader Checkpoint:       5600                              │    │
│  │                                                              │    │
│  │  Replication Lag:         120 operations (0.5 seconds)     │    │
│  │  Last Fetch:              2026-05-01 10:30:14              │    │
│  │  Retention Lease Renewed: 2026-05-01 10:30:15              │    │
│  │                                                              │    │
│  │  Status: ✓ HEALTHY                                          │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Alerts & Warnings                                                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ⚠️  Warning: Translog size growing on leader-02                     │
│      Lease from dead-cluster not removed                            │
│      Action: Manually remove stale lease                            │
│                                                                       │
│  ✓  All replications healthy                                        │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

This provides a complete visual reference for understanding retention leases!
